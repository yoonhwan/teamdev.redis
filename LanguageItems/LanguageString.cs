using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeamDev.Redis.Interface;

namespace TeamDev.Redis.LanguageItems
{
  public class LanguageString : ILanguageItem, IComplexItem
  {
    internal string _name;
    internal RedisDataAccessProvider _provider;

    public string Get()
    {
      return _provider.ReadString(_provider.SendCommand(RedisCommand.GET, _name));
    }

    public void Set(string value)
    {
      _provider.WaitComplete(_provider.SendCommand(RedisCommand.SET, _name, value));
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
