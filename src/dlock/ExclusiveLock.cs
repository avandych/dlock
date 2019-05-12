using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using org.apache.zookeeper;
using org.apache.zookeeper.data;
using static org.apache.zookeeper.KeeperException;

namespace dlock
{
    public class ExclusiveLock : IExclusiveLock
    {
        private readonly ILogger logger;
        private readonly ZooKeeper zooKeeper;
        private readonly ConnectedWatcher connectedWatcher;
        private readonly string lockPath;
        private readonly int timeoutMs;
        private readonly CancellationTokenSource cancellationTokenSource;
        private readonly CancellationTokenSource cancellationTokenSourceTimeout;

        public ExclusiveLock(string connectionString, string lockPath, int connectTimeoutMs, CancellationToken cancellationToken, ILogger logger)
        {
            CancellationTokenSource connectionLostCancellationTokenSource = new CancellationTokenSource();
            ConnectedWatcher watcher = new ConnectedWatcher(connectionLostCancellationTokenSource);

			this.timeoutMs = connectTimeoutMs;

			this.zooKeeper = new ZooKeeper(connectionString, timeoutMs, watcher);
            this.connectedWatcher = watcher;
            this.lockPath = lockPath;
            this.cancellationTokenSourceTimeout = new CancellationTokenSource();
            this.cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(connectionLostCancellationTokenSource.Token, cancellationToken);
            this.logger = logger;
        }

        public Task<(bool result, bool owner, long fencingToken, CancellationToken cancellationToken)> Wait(bool acquireLockOnConnectionFail = false)
        {
            var cancellationToken = cancellationTokenSource.Token;

            // if zookeeper client was unable to connect - respond with acquireLockOnConnectionFail
            if (this.connectedWatcher.Wait(TimeSpan.FromMilliseconds(this.timeoutMs), cancellationToken) == null)
            {
                return Task.FromResult((acquireLockOnConnectionFail, acquireLockOnConnectionFail, long.MinValue, CancellationToken.None));
            }

            // set timeout for lock acquiring
            this.cancellationTokenSourceTimeout.CancelAfter(this.timeoutMs);

            return this.WaitImpl(acquireLockOnConnectionFail, cancellationToken);
        }

        private async Task<(bool result, bool owner, long fencingToken, CancellationToken cancellationToken)> WaitImpl(bool acquireLockOnConnectionFail, CancellationToken cancellationToken)
        {
            string lockName = null;
            Stat stat = null;

            bool lockZnodeCreated = false;

            var exeptions = new List<KeeperException>(); 

            while (!cancellationToken.IsCancellationRequested && !this.cancellationTokenSourceTimeout.IsCancellationRequested)
            {
                try
                {
                    // create persistent znode for lock
                    if (!lockZnodeCreated)
                    {
                        lockZnodeCreated = await this.EnsurePathExists(lockPath);
                    }

                    // create ephemeral znode which will act as actual lock
                    if (string.IsNullOrWhiteSpace(lockName))
                    {
                        lockName = await this.zooKeeper.createAsync($"{lockPath}/lock-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                    }

                    // get stat for created node, czxid will be used as fencing token
                    if (stat == null)
                    {
                        stat = await this.zooKeeper.existsAsync(lockName);
                    }

                    (bool result, bool owner, CancellationToken cancellationToken) result = await this.Acquire(lockName, cancellationToken);

                    return (result.result, result.owner, stat.getCzxid(), result.cancellationToken);
                }
                catch (ConnectionLossException ex)
                {
                    this.logger.LogWarning(ex, $"Connection to Zookeeper ensemble lost while trying to acquire lock {lockName}, will try to reconnect");
                    continue;
                }
                catch (SessionExpiredException ex)
                {
                    this.logger.LogError(ex, $"Zookeeper session expired while trying to acquire lock {lockName}, will shutdown acquire procedure");
                    break;
                }
                catch (KeeperException ex)
                {
                    exeptions.Add(ex);
                    continue;
                }
            }

            if (exeptions.Any())
            {
                this.logger.LogTrace(new AggregateException(exeptions), $"Zookeeper error while trying to acquire lock {lockName}");
            }

            return (false, acquireLockOnConnectionFail, long.MinValue, CancellationToken.None);
        }

        private async Task<bool> EnsurePathExists(string path)
        {
            var znodes = path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            var fullPath = "";
            foreach (var znode in znodes)
            {
                fullPath = $"{fullPath}/{znode}";
                await this.EnsurePersistentZnodeExists(fullPath);
            }

            return true;
        }

        private async Task EnsurePersistentZnodeExists(string path)
        {
            try
            {
                await this.zooKeeper.createAsync(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch (NodeExistsException)
            {
            }
        }

        private async Task<(bool result, bool owner, CancellationToken cancellationToken)> Acquire(string lockName, CancellationToken cancellationToken)
        {
            bool owner = true;
            while (!cancellationToken.IsCancellationRequested && !this.cancellationTokenSourceTimeout.IsCancellationRequested)
            {
                // get all locks
                var locks = await this.zooKeeper.getChildrenAsync(this.lockPath, false);

                var allLocks = new SortedSet<string>();
                var lessThenMe = new SortedSet<string>();

                locks.Children.ForEach(l => allLocks.Add(l));
                foreach (var l in allLocks)
                {
                    if (!lockName.Equals($"{lockPath}/{l}"))
                    {
                        lessThenMe.Add(l);
                    }
                    else
                    {
                        break;
                    }
                }

                if (!lessThenMe.Any())
                {
                    return (true, owner, cancellationToken);
                }

                owner = false;

                var watcher = new WaitWatcher();
                var existsStat = await this.zooKeeper.existsAsync($"{lockPath}/{lessThenMe.Max}", watcher);

                // znode was deleted before we set watch
                if (existsStat == null)
                {
                    continue;
                }

                var waitResult = watcher.Wait(TimeSpan.FromMilliseconds(this.timeoutMs), cancellationToken);

                // we waited for whole timeout and lock wasn't released
                if (waitResult == null)
                {
                    break;
                }
            }

            return (false, false, CancellationToken.None);
        }

        public async Task Release()
        {
            this.cancellationTokenSource.Cancel();
            await this.zooKeeper?.closeAsync();
            return;
        }

        public void Dispose()
        {
            this.Release().Wait();
        }

        ~ExclusiveLock()
        {
            this.Dispose();
        }
    }
}
