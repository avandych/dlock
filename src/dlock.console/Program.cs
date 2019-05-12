using System;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace dlock.console
{
    class Program
    {
        static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging();

            var serviceProvider = serviceCollection.BuildServiceProvider();
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
            var cancellationTokenSource = new CancellationTokenSource();

            using (var exclusiveLock = new ExclusiveLock("localhost:2181", "/dlock-example", 5000, cancellationTokenSource.Token, loggerFactory.CreateLogger("dlock.console")))
            {
                var dlock = exclusiveLock.Wait().GetAwaiter().GetResult();

                Console.WriteLine(dlock);

                if (dlock.result)
                {
                    SpinWait.SpinUntil(() => dlock.cancellationToken.IsCancellationRequested);

                    Console.WriteLine("Releasing lock, cancellation requested");
                }
            }
        }
    }
}
