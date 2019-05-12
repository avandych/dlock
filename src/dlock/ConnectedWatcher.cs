using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace dlock
{
    public class ConnectedWatcher : WaitWatcher
    {
        private readonly CancellationTokenSource cts; 

        public ConnectedWatcher(CancellationTokenSource cts)
        {
            this.cts = cts;
        }

        public async override Task process(WatchedEvent @event)
        {
            var keeperState = @event.getState();

            if (keeperState == Event.KeeperState.SyncConnected)
            {
                await base.process(@event);
            }
            else if (keeperState == Event.KeeperState.Expired || keeperState == Event.KeeperState.Disconnected)
            {
                this.cts.Cancel();
            }
        }
    }
}
