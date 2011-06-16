using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TeamDev.Redis.Test
{
  /// <summary>
  /// Summary description for UsersTests
  /// </summary>
  [TestClass]
  public class UsersTests
  {
    private RedisDataAccessProvider _redis = new RedisDataAccessProvider();

    public UsersTests()
    {
      //_redis.Configuration.Host = "192.168.1.81";

    }

    private TestContext testContextInstance;

    /// <summary>
    ///Gets or sets the test context which provides
    ///information about and functionality for the current test run.
    ///</summary>
    public TestContext TestContext
    {
      get
      {
        return testContextInstance;
      }
      set
      {
        testContextInstance = value;
      }
    }

    #region Additional test attributes
    //
    // You can use the following additional attributes as you write your tests:
    //
    // Use ClassInitialize to run code before running the first test in the class
    // [ClassInitialize()]
    // public static void MyClassInitialize(TestContext testContext) { }
    //
    // Use ClassCleanup to run code after all tests in a class have run
    // [ClassCleanup()]
    // public static void MyClassCleanup() { }
    //
    // Use TestInitialize to run code before running each test 
    // [TestInitialize()]
    // public void MyTestInitialize() { }
    //
    // Use TestCleanup to run code after each test has run
    // [TestCleanup()]
    // public void MyTestCleanup() { }
    //
    #endregion

    [TestMethod]
    public void ExistsAfter_DEL_SREM_Bug_SIMULATION1()
    {
      _redis.Configuration.LogUnbalancedCommands = true;

      _redis.WaitComplete(_redis.SendCommand(RedisCommand.FLUSHALL));

      _redis.Key.Exists("TEST");

      Dictionary<string, string> _values = new Dictionary<string, string>();
      _values.Add("v1", "testvalue1");
      _values.Add("v2", "testvalue2");
      _values.Add("v3", "testvalue3");
      _values.Add("v4", "testvalue4");

      _redis.Hash["TEST"].Set(_values);

      _redis.Key.Exists("TEST");
    }

    [TestMethod]
    public void ExistsAfter_DEL_SREM_Bug_SIMULATION2()
    {
      _redis.Configuration.LogUnbalancedCommands = true;
      _redis.WaitComplete(_redis.SendCommand(RedisCommand.FLUSHALL));

      _redis.Key.Exists("TEST");
      _redis.Set["MySet"].Add("setvalue1");
      _redis.Set["MySet"].Add("setvalue1");
      _redis.Set["MySet"].Add("setvalue2");
      _redis.Set["MySet"].Add("setvalue3");
      _redis.Set["MySet"].Add("setvalue4");

      Dictionary<string, string> _values = new Dictionary<string, string>();
      _values.Add("v1", "testvalue1");
      _values.Add("v2", "testvalue2");
      _values.Add("v3", "testvalue3");
      _values.Add("v4", "testvalue4");

      _redis.Hash["TEST"].Set(_values);

      _redis.Hash["TEST"].Get("v1", "v2", "v3", "v4");

      _redis.Key.Exists("TEST");

      foreach (var v in _redis.Set["MySet"].Members)
      {
        _redis.Set["MySet"].Remove(v);
      }

      _redis.Key.Exists("TEST");
      _redis.Key.Exists("MySet");

      _redis.Set["MySet"].Clear();

      _redis.Key.Exists("MySet");
    }

    [TestMethod]
    public void ExistsAfter_DEL_SREM_Bug_SIMULATION3()
    {
      _redis.Configuration.LogUnbalancedCommands = true;
      _redis.WaitComplete(_redis.SendCommand(RedisCommand.FLUSHALL));

      for (int x = 0; x < 10000; x++)
      {
        _redis.Key.Exists("TEST");
        _redis.Set["MySet"].Add("setvalue1");
        _redis.Set["MySet"].Add("setvalue1");
        _redis.Set["MySet"].Add("setvalue2");
        _redis.Set["MySet"].Add("setvalue3");
        _redis.Set["MySet"].Add("setvalue4");

        Dictionary<string, string> _values = new Dictionary<string, string>();
        _values.Add("v1", "testvalue1");
        _values.Add("v2", "testvalue2");
        _values.Add("v3", "testvalue3");
        _values.Add("v4", "<372189371,389217838921,3218372189>");

        _redis.Hash["TEST"].Set(_values);

        _redis.Hash["TEST"].Get("v1", "v2", "v3", "v4");

        _redis.Key.Exists("TEST");

        foreach (var v in _redis.Set["MySet"].Members)
        {
          _redis.Set["MySet"].Remove(v);
          _redis.Set["MySet"].Remove(v);
          _redis.Key.Remove("TEST");
        }

        _redis.Key.Exists("TEST");
        _redis.Key.Exists("MySet");

        _redis.Set["MySet"].Clear();
        _redis.Key.Remove("TEST");

        _redis.Key.Exists("MySet");
      }
    }
  }
}
