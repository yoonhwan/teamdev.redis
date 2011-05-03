using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using TeamDev.Redis.Interface;

namespace TeamDev.Redis.LanguageItems
{
  public class LanguageSet : ILanguageItem, IComplexItem
  {
    internal string _name;
    internal RedisDataAccessProvider _provider;

    public bool Add(string value)
    {
      _provider.SendCommand(RedisCommand.SADD, _name, value);
      return _provider.ReadInt() == 1;
    }

    public void Clear()
    {
      _provider.SendCommand(RedisCommand.DEL, _name);
      _provider.WaitComplete();
    }

    public bool IsMember(string value)
    {
      _provider.SendCommand(RedisCommand.SISMEMBER, _name, value);
      return _provider.ReadInt() == 1;
    }

    public string[] Members
    {
      get
      {
        _provider.SendCommand(RedisCommand.SMEMBERS, _name);
        return _provider.ReadMultiString();
      }
    }

    public void Remove(string value)
    {
      _provider.SendCommand(RedisCommand.SREM, _name, value);
      _provider.WaitComplete();
    }

    [Description("SUNION -> Returns the members of the set resulting from the union of all the given sets")]
    public string[] Union(params string[] sets)
    {
      List<string> args = new List<string>();
      args.Add(_name);
      args.AddRange(sets);

      _provider.SendCommand(RedisCommand.SUNION, args.ToArray());
      return _provider.ReadMultiString();
    }

    [Description("SINTER -> Returns the members of the set resulting from the intersection of all the given sets")]
    public string[] Intersect(params string[] sets)
    {
      List<string> args = new List<string>();
      args.Add(_name);
      args.AddRange(sets);

      _provider.SendCommand(RedisCommand.SINTER, args.ToArray());
      return _provider.ReadMultiString();
    }

    [Description("SMOVE -> Move member from the set at source to the set at destination. This operation is atomic")]
    public bool Move(string destination, string value)
    {
      _provider.SendCommand(RedisCommand.SMOVE, _name, destination, value);
      return _provider.ReadInt() == 1;
    }

    [Description("SDIF -> Returns the members of the set resulting from the difference between the first set and all the successive sets")]
    public string[] Subtract(params string[] sets)
    {
      List<string> args = new List<string>();
      args.Add(_name);
      args.AddRange(sets);

      _provider.SendCommand(RedisCommand.SDIFF, args.ToArray());
      return _provider.ReadMultiString();

    }

    public int Count
    {
      get
      {
        _provider.SendCommand(RedisCommand.SCARD, _name);
        return _provider.ReadInt();
      }
    }

    [Description("SPOP -> Removes and returns a random element from the set value stored at key")]
    public string Pop()
    {
      _provider.SendCommand(RedisCommand.SPOP, _name);
      return _provider.ReadString();
    }

    [Description("SRANDMEMBER -> Return a random element from the set value stored at key without removing it from set")]
    public string Random
    {
      get
      {
        _provider.SendCommand(RedisCommand.SRANDMEMBER, _name);
        return _provider.ReadString();
      }
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
