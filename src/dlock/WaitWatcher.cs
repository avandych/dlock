using System;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace dlock
{
    public class WaitWatcher : Watcher
    {
        private readonly ManualResetEventSlim manualResetEvent = new ManualResetEventSlim(false);

        public WatchedEvent WatchedEvent { get; private set; }

        public WatchedEvent Wait(TimeSpan timeout, CancellationToken cancellationToken)
        {
            this.manualResetEvent.Wait(timeout, cancellationToken);
            return this.WatchedEvent;
        }

        public void Reset()
        {
            this.manualResetEvent.Reset();
        }


        public override Task process(WatchedEvent @event)
        {
            this.WatchedEvent = @event;
            this.manualResetEvent.Set();

            return Task.CompletedTask;
        }
    }

}
