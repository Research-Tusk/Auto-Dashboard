/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  experimental: {
    typedRoutes: true,
  },
  // Allow Supabase image domain for any future OEM logo support
  images: {
    domains: ['supabase.co'],
  },
};

module.exports = nextConfig;
