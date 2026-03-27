export default async function Home() {
  const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/items`, {
    cache: "no-store",
  });
  const data = await res.json();

  return (
    <main className="p-8">
      <h1 className="text-2xl font-bold mb-4">Items from FastAPI + Supabase</h1>
      <ul className="list-disc pl-6">
        {data.items.map((item: { id: string; name: string }) => (
          <li key={item.id}>{item.name}</li>
        ))}
      </ul>
    </main>
  );
}