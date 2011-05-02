using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using TeamDev.Redis.Interface;

namespace TeamDev.Redis.LanguageItems
{
  public class LanguageSortedSet : ILanguageItem, IComplexItem
  {
    internal string _name;
    internal RedisDataAccessProvider _provider;

    [Obsolete("Please use the overload with double typed score parameter")]
    public bool Add(string score, string member)
    {
      _provider.InternalSendCommand(RedisCommand.ZADD, _name, score, member);
      return _provider.ReadInt() == 1;
    }

    public bool Add(double score, string member)
    {
      _provider.InternalSendCommand(RedisCommand.ZADD, _name, score.ToString(System.Globalization.CultureInfo.InvariantCulture), member);
      return _provider.ReadInt() == 1;
    }


    [Description("Returns the sorted set cardinality (number of elements) of the sorted set")]
    public int Cardinality
    {
      get
      {
        _provider.InternalSendCommand(RedisCommand.ZCARD, _name);
        return _provider.ReadInt();
      }
    }

    [Description("Returns the number of elements in the sorted set at key with a score between min and max")]
    public int Count(string min, string max)
    {
      _provider.InternalSendCommand(RedisCommand.ZCOUNT, _name, min, max);
      return _provider.ReadInt();
    }

    public string[] IncrementBy(string member, int incrementvalue)
    {
      _provider.InternalSendCommand(RedisCommand.ZINCRBY, _name, incrementvalue.ToString(), member);
      return _provider.ReadMultiString();
    }

    public string[] Range(string min, string max)
    {
      _provider.InternalSendCommand(RedisCommand.ZRANGE, _name, min, max);
      return _provider.ReadMultiString();
    }

    public int Rank(string member)
    {
      _provider.InternalSendCommand(RedisCommand.ZRANK, _name, member);
      return _provider.ReadInt();
    }

    public void Clear()
    {
      _provider.InternalSendCommand(RedisCommand.DEL, _name);
      _provider.WaitComplete();
    }

    void ILanguageItem.Configure(string name, RedisDataAccessProvider provider)
    {
      _name = name;
      _provider = provider;
    }

    string IComplexItem.KeyName
    {
      get { return _name; }
    }

    RedisDataAccessProvider IComplexItem.Provider
    {
      get { return _provider; }
    }


  }
}
