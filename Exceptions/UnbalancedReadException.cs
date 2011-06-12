using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeamDev.Redis
{
  public class UnbalancedReadException : ApplicationException
  {
    public UnbalancedReadException()
      : base()
    { }

    public UnbalancedReadException(string message)
      : base(message)
    { }
  }
}
