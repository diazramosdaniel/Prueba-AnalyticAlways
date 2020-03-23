using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Lector_AnalyticAlways
{
    public class LectorAnalyticAlwaysException : Exception
    {
        public LectorAnalyticAlwaysException()
        {
        }

        public LectorAnalyticAlwaysException(string message) : base(message)
        {
        }

        public LectorAnalyticAlwaysException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected LectorAnalyticAlwaysException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
