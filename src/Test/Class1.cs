namespace Test;

using BitBadger.Documents;

public class Class1
{
    public void Toot()
    {
        var ticket = Query.WhereByField("test", Op.GE, "");
        var others = Query.Count.All("howdy");
    }
}
