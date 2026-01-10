module.exports = {
  content: ["./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#0f172a",
        slate: "#475569",
        mist: "#e2e8f0",
        ember: "#f97316"
      },
      backgroundImage: {
        "mesh": "radial-gradient(circle at top, rgba(15, 23, 42, 0.12), transparent 55%), radial-gradient(circle at 20% 20%, rgba(249, 115, 22, 0.18), transparent 45%)"
      }
    }
  },
  plugins: []
};
