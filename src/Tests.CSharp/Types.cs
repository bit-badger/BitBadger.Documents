namespace BitBadger.Documents.Tests.CSharp;

public class SubDocument
{
    public string Foo { get; set; } = "";
    public string Bar { get; set; } = "";
}

public class JsonDocument
{
    public string Id { get; set; } = "";
    public string Value { get; set; } = "";
    public int NumValue { get; set; } = 0;
    public SubDocument? Sub { get; set; } = null;
}
