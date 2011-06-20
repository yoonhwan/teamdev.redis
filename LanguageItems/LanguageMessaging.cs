using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeamDev.Redis.Interface;
using System.ComponentModel;

namespace TeamDev.Redis.LanguageItems
{
  public class LanguageMessaging : ILanguageItem, IComplexItem
  {
    internal string _name;
    internal RedisDataAccessProvider _provider;

    [Description(CommandDescriptions.PUBLISH)]
    public int Publish(string channel, string message)
    {
      return _provider.ReadInt(_provider.SendCommand(RedisCommand.PUBLISH, channel, message));
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
