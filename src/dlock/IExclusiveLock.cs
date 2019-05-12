using System;
using System.Threading;
using System.Threading.Tasks;

namespace dlock
{
    public interface IExclusiveLock : IDisposable
    {
        Task<(bool result, bool owner, long fencingToken, CancellationToken cancellationToken)> Wait(bool acquireLockOnConnectionFail = true);
        Task Release();
    }
}
