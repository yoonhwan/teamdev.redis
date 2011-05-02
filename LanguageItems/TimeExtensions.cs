using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeamDev.Redis.Interface;


namespace TeamDev.Redis.LanguageItems
{
  public static class TimeExtensions
  {
    public static bool Expire(this IComplexItem item, int seconds)
    {
      item.Provider.SendCommand(RedisCommand.EXPIRE, item.KeyName, seconds.ToString());
      return item.Provider.ReadInt() == 1;
    }

    public static bool Persist(this IComplexItem item)
    {
      item.Provider.SendCommand(RedisCommand.EXPIRE, item.KeyName);
      return item.Provider.ReadInt() == 1;
    }

    //public static bool Expire(this IComplexItem item, DateTime time)
    //{
    //  item.Provider.InternalSendCommand(Provides.RedisCommand.EXPIRE, item.KeyName, time.ToUniversalTime);
    //  return item.Provider.ReadInt() == 1;
    //}

    public static int TTL(this IComplexItem item)
    {
      item.Provider.SendCommand(RedisCommand.TTL, item.KeyName);
      return item.Provider.ReadInt();
    }
  }
}
