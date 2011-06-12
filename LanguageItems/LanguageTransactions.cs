using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeamDev.Redis.Interface;

namespace TeamDev.Redis.LanguageItems
{
  public class LanguageTransactions : ILanguageItem, IComplexItem
  {
    internal string _name;
    internal RedisDataAccessProvider _provider;

    public void Begin()
    {
      _provider.WaitComplete(_provider.SendCommand(RedisCommand.MULTI));
    }

    public string[] Commit()
    {
      return _provider.ReadMultiString(_provider.SendCommand(RedisCommand.EXEC));
    }

    public void Rollback()
    {
      _provider.WaitComplete(_provider.SendCommand(RedisCommand.DISCARD));
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
