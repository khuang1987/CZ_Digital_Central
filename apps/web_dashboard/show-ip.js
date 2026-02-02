const os = require('os');
const networkInterfaces = os.networkInterfaces();

console.log('\n--- Network Connection Info ---');
Object.keys(networkInterfaces).forEach((interfaceName) => {
    networkInterfaces[interfaceName].forEach((iface) => {
        // Skip over internal (non-IPv4) and localhost addresses
        if (iface.family === 'IPv4' && !iface.internal) {
            console.log(`🌐 Dashboard URL: http://${iface.address}:3000`);
        }
    });
});
console.log('-------------------------------\n');
