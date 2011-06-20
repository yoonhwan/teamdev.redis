using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;

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
      _redis.Configuration.Host = "192.168.1.81";

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

    [TestInitialize()]
    public void MyTestInitialize()
    {
      _redis.WaitComplete(_redis.SendCommand(RedisCommand.FLUSHALL));
    }

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

    [TestMethod]
    public void ExistsAfter_DEL_SREM_Bug_SIMULATION4()
    {
      _redis.Configuration.LogUnbalancedCommands = true;

      // Based on Log received from Farland game. 

      Thread.Sleep(10 * 1000);

      var m = _redis.Set["users"].Members;
      var c = _redis.Set["characters"].Members;

      // Character creation
      dynamic response = _redis.Key.Exists("user_5");
      _redis.Set["characters"].Add("30");
      SetCharacter(30, 5);

      _redis.Set["users"].Add("5");

      Dictionary<string, string> usersdata = new Dictionary<string, string>();
      usersdata.Add("username", "testuser");
      usersdata.Add("userfullname", "testuserfullname");
      _redis.Hash["user_5"].Set(usersdata);


      // Update character position
      UpdatePosition(30, 5);
      UpdatePosition(30, 5);
      UpdatePosition(30, 5);

      _redis.Key.Exists("user_5");

      UpdatePosition(30, 5);
      UpdatePosition(30, 5);

      GetCharacterData(30);

      _redis.Set["characters"].Remove("30");
      _redis.Hash["characters_30"].Clear();      

      _redis.Set["users"].Remove("5");
      _redis.Hash["user_5"].Clear();

      Thread.Sleep(60 * 1000);

      _redis.Key.Exists("user_5");
    }

    [TestMethod]
    public void ExistsAfter_DEL_SREM_Bug_SIMULATION5()
    {
      for (int i = 0; i < 1000; i++)
        ExistsAfter_DEL_SREM_Bug_SIMULATION4();
    }

    private void SetCharacter(int id, int userid)
    {
      Dictionary<string, string> dataKeyValue = new Dictionary<string, string>();
      dataKeyValue.Add("character_id", id.ToString());
      dataKeyValue.Add("login_id", userid.ToString());
      dataKeyValue.Add("character_name", "test");
      dataKeyValue.Add("race_id", "1");
      dataKeyValue.Add("class_id", "2");
      dataKeyValue.Add("faction_id", "3");
      dataKeyValue.Add("character_level", "4");
      dataKeyValue.Add("character_exp", "5");
      dataKeyValue.Add("attribute_points", "6");
      dataKeyValue.Add("might_dist", "7");
      dataKeyValue.Add("constitution_dist", "8");
      dataKeyValue.Add("dexterity_dist", "9");
      dataKeyValue.Add("agility_dist", "10");
      dataKeyValue.Add("intelligence_dist", "11");
      dataKeyValue.Add("willpower_dist", "12");
      dataKeyValue.Add("charisma_dist", "13");
      dataKeyValue.Add("luck_dist", "14");
      dataKeyValue.Add("might_max", "15");
      dataKeyValue.Add("constitution_max", "16");
      dataKeyValue.Add("dexterity_max", "17");
      dataKeyValue.Add("agility_max", "18");
      dataKeyValue.Add("intelligence_max", "19");
      dataKeyValue.Add("willpower_max", "20");
      dataKeyValue.Add("charisma_max", "21");
      dataKeyValue.Add("luck_max", "22");
      dataKeyValue.Add("health_max", "23");
      dataKeyValue.Add("mana_max", "24");
      dataKeyValue.Add("stamina_max", "25");
      dataKeyValue.Add("health_current", "26");
      dataKeyValue.Add("mana_current", "27");
      dataKeyValue.Add("stamina_current", "28");
      dataKeyValue.Add("last_position", "29");
      dataKeyValue.Add("last_rotation", "30");
      dataKeyValue.Add("last_zone", "31");
      _redis.Hash["character_" + id.ToString()].Set(dataKeyValue);
    }

    public void GetCharacterData(int id)
    {
      string[] characterFields = { "character_id", 
                                         "login_id", 
                                         "character_name",
                                         "race_id",
                                         "class_id",
                                         "faction_id",
                                         "character_level",
                                         "character_exp",
                                         "attribute_points",
                                         "might_dist",
                                         "constitution_dist",
                                         "dexterity_dist",
                                         "agility_dist",
                                         "intelligence_dist",
                                         "willpower_dist",
                                         "charisma_dist",
                                         "luck_dist",
                                         "might_max",
                                         "constitution_max",
                                         "dexterity_max",
                                         "agility_max",
                                         "intelligence_max",
                                         "willpower_max",
                                         "charisma_max",
                                         "luck_max",
                                         "health_max",
                                         "mana_max",
                                         "stamina_max",
                                         "health_current",
                                         "mana_current",
                                         "stamina_current",
                                         "last_position",
                                         "last_rotation",
                                         "last_zone"};
      string[] data = _redis.Hash["character_" + id].Get(characterFields);
    }

    private void UpdatePosition(int id, int userid)
    {
      if (_redis.Key.Exists("user_" + userid))
      {

        Random rnd = new Random();

        Dictionary<string, string> newcharacterdata = new Dictionary<string, string>();
        newcharacterdata.Add("character_id", id.ToString());
        newcharacterdata.Add("last_position", ((float)(rnd.NextDouble() * 360)).ToString(System.Globalization.CultureInfo.InvariantCulture));
        newcharacterdata.Add("last_rotation", ((float)(rnd.NextDouble() * 360)).ToString(System.Globalization.CultureInfo.InvariantCulture));
        _redis.Hash["character_" + id.ToString()].Set(newcharacterdata);
      }
    }
  }
}
