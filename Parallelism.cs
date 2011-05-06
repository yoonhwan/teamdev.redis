using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace TeamDev.Redis
{
  public static class Parallelism
  {
    private static long _workingthreads = 0;
    private static long _requiredtimes = 0;

    private static Thread[] _threads = null;
    private static ManualResetEventSlim _actionsEnqueued = new ManualResetEventSlim(false);
    private static ManualResetEventSlim _actionCompleted = new ManualResetEventSlim(false);
    private static Queue<Action> _actions = new Queue<Action>();

    public static int ThreadsCount { get; private set; }

    static Parallelism()
    {
      SetMaxThreads((Environment.ProcessorCount * 3) + (Environment.ProcessorCount / 2));
    }

    public static void SetMaxThreads(int count)
    {
      ThreadsCount = count;
      _threads = new Thread[ThreadsCount];

      for (int x = 0; x < ThreadsCount; x++)
        _threads[x] = InitThread();
    }

    private static Thread InitThread()
    {
      var tt = new Thread(new ThreadStart(() =>
        {
          while (true)
          {
            _actionsEnqueued.Wait();

            Action nextaction = null;

            lock (_actions)
            {
              if (_actions.Count > 0) nextaction = _actions.Dequeue();
            }

            if (nextaction != null)
            {
              if (Interlocked.Read(ref _requiredtimes) > 0)
              {
                Interlocked.Decrement(ref _requiredtimes);

                Interlocked.Increment(ref _workingthreads);
                nextaction.Invoke();
                Interlocked.Decrement(ref _workingthreads);

              }
            }

            _actionCompleted.Set();
          }
        })) { IsBackground = true };

      tt.Start();
      return tt;
    }

    public static void ParallelForEach<T>(this IEnumerable<T> items, Action<T> action)
    {
      Queue<T> datas = new Queue<T>();
      foreach (var item in items)
      {
        datas.Enqueue(item);
        Interlocked.Increment(ref _requiredtimes);
        _actions.Enqueue(() => action(datas.Dequeue()));
      }

      _actionCompleted.Reset();
      _actionsEnqueued.Set();
      WaitCompleted();
    }

    public static void NotifyDataEnqueued()
    {      
      
    }

    public static void WaitCompleted(int timeout = -1)
    {
      while (true)
      {
        _actionCompleted.Wait(timeout);
        //_actionCompleted.Reset();
        if (Interlocked.Read(ref _workingthreads) == 0)
        {
          _actionsEnqueued.Reset();
          return;
        }
        _actionCompleted.Reset();
      }
    }
  }
}
