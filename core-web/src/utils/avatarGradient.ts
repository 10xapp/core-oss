const GRADIENTS = [
  ['#9BA3F5', '#B4A4F7'], // indigo → violet
  ['#E8A0C0', '#F0A0A8'], // pink → rose
  ['#E8C878', '#E89898'], // amber → red
  ['#78C8A8', '#90D0C0'], // emerald → teal
  ['#88B8E8', '#9BA3F5'], // blue → indigo
  ['#B4A4F7', '#E8A0C0'], // violet → pink
  ['#90D0C0', '#80C8D8'], // teal → cyan
  ['#E8A880', '#E8C878'], // orange → amber
  ['#80C8D8', '#88B8E8'], // cyan → blue
  ['#E89898', '#E8A880'], // red → orange
  ['#B0D080', '#78C8A8'], // lime → emerald
  ['#C8A0E8', '#9BA3F5'], // purple → indigo
];

function hash(str: string): number {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = ((h << 5) - h + str.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

export function avatarGradient(name: string): string {
  const [from, to] = GRADIENTS[hash(name) % GRADIENTS.length];
  return `linear-gradient(135deg, ${from} 0%, ${to} 100%)`;
}
