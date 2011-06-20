using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeamDev.Redis.Interface
{
  public interface IComplexItem
  {
    string KeyName { get; }
    RedisDataAccessProvider Provider { get; }
  }
}
