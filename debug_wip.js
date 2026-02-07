async function test() {
    try {
        const res = await fetch('http://localhost:3000/api/delivery/wip');
        const json = await res.json();
        console.log(JSON.stringify(json, null, 2));
    } catch (err) {
        console.error('Fetch Error:', err);
    }
}
test();
