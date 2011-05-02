using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.ComponentModel;
using TeamDev.Redis.Interface;

namespace TeamDev.Redis.LanguageItems
{
  public class LanguageKey : ILanguageItem
  {
    internal string _name;
    internal RedisDataAccessProvider _provider;

    public bool Remove(params string[] keys)
    {
      _provider.InternalSendCommand(RedisCommand.DEL, keys);
      return _provider.ReadInt() == 1;
    }

    public bool Exists(string key)
    {
      _provider.InternalSendCommand(RedisCommand.EXISTS, key);
      return _provider.ReadInt() == 1;
    }

    [Description("Return all keys matching the pattern")]
    public string[] this[string pattern]
    {
      get
      {
        _provider.InternalSendCommand(RedisCommand.KEYS, pattern);
        return _provider.ReadMultiString();
      }
    }

    public string Type(string key)
    {
      _provider.InternalSendCommand(RedisCommand.TYPE, key);
      return _provider.ReadString();
    }

    void ILanguageItem.Configure(string name, RedisDataAccessProvider provider)
    {
      _name = name;
      _provider = provider;
    }
  }
}
