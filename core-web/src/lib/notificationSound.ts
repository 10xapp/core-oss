/**
 * Notification sound utility using Web Audio API.
 * Plays a subtle, pleasant notification tone without requiring external files.
 */

let audioContext: AudioContext | null = null;

function getAudioContext(): AudioContext | null {
  if (typeof window === 'undefined') return null;

  if (!audioContext) {
    try {
      audioContext = new (window.AudioContext || (window as any).webkitAudioContext)();
    } catch {
      console.warn('Web Audio API not supported');
      return null;
    }
  }
  return audioContext;
}

/**
 * Plays a subtle notification sound for new messages.
 * Uses two quick tones for a pleasant "ping" effect.
 */
function playTone(ctx: AudioContext): void {
  const now = ctx.currentTime;
  const volume = 0.15;

  const osc1 = ctx.createOscillator();
  const gain1 = ctx.createGain();
  osc1.connect(gain1);
  gain1.connect(ctx.destination);
  osc1.frequency.value = 880; // A5
  osc1.type = 'sine';
  gain1.gain.setValueAtTime(volume, now);
  gain1.gain.exponentialRampToValueAtTime(0.001, now + 0.1);
  osc1.start(now);
  osc1.stop(now + 0.1);

  const osc2 = ctx.createOscillator();
  const gain2 = ctx.createGain();
  osc2.connect(gain2);
  gain2.connect(ctx.destination);
  osc2.frequency.value = 659; // E5
  osc2.type = 'sine';
  gain2.gain.setValueAtTime(0, now);
  gain2.gain.setValueAtTime(volume, now + 0.08);
  gain2.gain.exponentialRampToValueAtTime(0.001, now + 0.2);
  osc2.start(now + 0.08);
  osc2.stop(now + 0.2);
}

/**
 * Plays a subtle notification sound for new messages.
 * Uses two quick tones for a pleasant "ping" effect.
 */
export function playMessageNotification(): void {
  const ctx = getAudioContext();
  if (!ctx) return;

  if (ctx.state === 'suspended') {
    // Await resume before scheduling oscillators so they don't fire into a dead context
    ctx.resume().then(() => playTone(ctx)).catch(() => {});
  } else {
    playTone(ctx);
  }
}
