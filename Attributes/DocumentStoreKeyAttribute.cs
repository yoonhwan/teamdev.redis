using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeamDev.Redis
{
  [AttributeUsage(AttributeTargets.Property)]
  public class DocumentStoreKeyAttribute : System.Attribute
  {
  }
}
