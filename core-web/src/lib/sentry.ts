import * as Sentry from '@sentry/react';
import { browserTracingIntegration } from '@sentry/react';

export function initSentry() {
  const dsn = import.meta.env.VITE_SENTRY_DSN;
  if (!dsn) return;

  Sentry.init({
    dsn,
    enabled: import.meta.env.PROD,
    integrations: [
      browserTracingIntegration(),
      // Captures console.error() calls as Sentry events — remove if too noisy
      Sentry.captureConsoleIntegration({ levels: ['error'] }),
      Sentry.feedbackIntegration({
        autoInject: true,
        showBranding: false,
        colorScheme: 'light',
        showName: false,
        showEmail: false,
        enableScreenshot: true,
        useSentryUser: { email: 'email', name: 'username' },
        triggerLabel: 'Feedback',
        formTitle: 'Send Feedback',
        messagePlaceholder: 'Describe the issue or share your feedback...',
        submitButtonLabel: 'Send',
        successMessageText: 'Thanks! Your feedback has been sent.',
        themeLight: {
          foreground: '#000000',
          background: '#FFFFFF',
          accentForeground: '#FFFFFF',
          accentBackground: '#000000',
          successColor: '#589F72',
          errorColor: '#EA6B6B',
        },
      }),
    ],
    tracesSampleRate: 0.05,
    tracePropagationTargets: [
      /^http:\/\/localhost/,
    ],
    beforeSend(event) {
      const message = event.exception?.values?.[0]?.value ?? '';
      // Drop offline noise (already handled by toast)
      if (/Failed to fetch|NetworkError|Load failed/i.test(message)) {
        return null;
      }
      // Drop browser ResizeObserver noise
      if (/ResizeObserver/i.test(message)) {
        return null;
      }
      return event;
    },
  });
}

export function setSentryUser(user: { id: string; email?: string; name?: string } | null) {
  if (user) {
    Sentry.setUser({ id: user.id, email: user.email, username: user.name });
  } else {
    Sentry.setUser(null);
  }
}

export function captureException(
  error: unknown,
  context?: Record<string, unknown>,
) {
  Sentry.captureException(error, context ? { extra: context } : undefined);
}

export { Sentry };
