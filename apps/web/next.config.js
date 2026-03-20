/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  async rewrites() {
    return [
      { source: "/analytics/:path*", destination: "/api/analytics/:path*" },
      { source: "/filters/:path*", destination: "/api/filters/:path*" },
      { source: "/network", destination: "/api/network" },
    ];
  },
};

module.exports = nextConfig;
