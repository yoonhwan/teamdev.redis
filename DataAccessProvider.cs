using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeamDev.Redis.LanguageItems;

namespace TeamDev.Redis
{
  public abstract class DataAccessProvider
  {
    public Configuration Configuration { get; set; }

    public DataAccessProvider()
    {
      Configuration = new LanguageItems.Configuration();
    }

    public abstract void Connect();
    public abstract void Close();

  }
}
