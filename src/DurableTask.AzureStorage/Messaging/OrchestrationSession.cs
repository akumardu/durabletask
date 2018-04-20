//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.AzureStorage.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;

    sealed class OrchestrationSession : SessionBase, IOrchestrationSession, IDisposable
    {
        readonly TimeSpan idleTimeout;

        readonly Queue<List<MessageData>> messageBuffer;
        readonly SemaphoreSlim messageBufferSemaphore;

        bool isShuttingDown;

        public OrchestrationSession(
            string storageAccountName,
            string taskHubName,
            OrchestrationInstance orchestrationInstance,
            TimeSpan idleTimeout)
            : base(storageAccountName, taskHubName, orchestrationInstance)
        {
            this.idleTimeout = idleTimeout;

            this.messageBuffer = new Queue<List<MessageData>>();
            this.messageBufferSemaphore = new SemaphoreSlim(initialCount: 0);
        }

        public IReadOnlyList<MessageData> CurrentMessageBatch { get; set; }

        public bool IsReleased { get; private set; }

        public override DateTime GetNextMessageExpirationTimeUtc()
        { 
            if (this.CurrentMessageBatch == null)
            {
                return DateTime.MinValue;
            }

            return this.CurrentMessageBatch.Min(msg => msg.OriginalQueueMessage.NextVisibleTime.Value.UtcDateTime);
        }

        public bool TryAddMessages(List<MessageData> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            if (messages.Count == 0)
            {
                throw new ArgumentException("The message list must not be empty.", nameof(messages));
            }

            this.CurrentMessageBatch = messages;

            lock (this.messageBuffer)
            {
                if (this.isShuttingDown)
                {
                    return false;
                }

                this.messageBuffer.Enqueue(messages);
                this.messageBufferSemaphore.Release();
                return true;
            }
        }

        public async Task<IList<TaskMessage>> FetchNewOrchestrationMessagesAsync(
            TaskOrchestrationWorkItem workItem)
        {
            if (this.isShuttingDown)
            {
                return null;
            }

            if (!await this.messageBufferSemaphore.WaitAsync(this.idleTimeout))
            {
                this.isShuttingDown = true;
            }

            lock (this.messageBuffer)
            {
                // Queue length of zero means we're shutting down. It's possible that more
                // messages were added after we timed out, in which case we'll check the
                // queue one more time for messages.
                if (this.messageBuffer.Count == 0)
                {
                    Debug.Assert(this.isShuttingDown, "The queue should be empty only during shutdown.");
                    return null;
                }

                List<MessageData> nextBatch = this.messageBuffer.Dequeue();

                var messages = new List<TaskMessage>(nextBatch.Count);
                foreach (MessageData msg in nextBatch)
                {
                    this.TraceMessageReceived(msg);
                    messages.Add(msg.TaskMessage);
                }

                return messages;
            }
        }

        public void Dispose()
        {
            this.messageBufferSemaphore.Dispose();
            this.IsReleased = true;
        }
    }
}
