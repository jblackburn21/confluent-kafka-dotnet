using Avro;
using Avro.Specific;

namespace MessageTypes
{
    public abstract class BaseEvent : ISpecificRecord 
    {
        public abstract Schema Schema { get; }

        public abstract object Get(int fieldPos);

        public abstract void Put(int fieldPos, object fieldValue);
    }
}