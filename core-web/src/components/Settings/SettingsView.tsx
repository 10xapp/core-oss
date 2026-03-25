import { useState, useEffect, useRef } from 'react';
import { CameraIcon, PlusIcon } from '@heroicons/react/24/outline';
import { avatarGradient } from '../../utils/avatarGradient';
import { useAuthStore } from '../../stores/authStore';
import { useEmailAccountsStore } from '../../stores/emailAccountsStore';
import Modal from '../Modal/Modal';
import { uploadAvatar, deleteAvatar } from '../../api/client';

interface SettingsViewProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function SettingsView({ isOpen, onClose }: SettingsViewProps) {
  const { isAuthenticated, user, userProfile, signInWithGoogle, signInWithMicrosoft, signOut, updateAvatarUrl, updateUserName } = useAuthStore();
  const [error, setError] = useState('');
  const [authLoadingProvider, setAuthLoadingProvider] = useState<'google' | 'microsoft' | null>(null);

  // Avatar upload state
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [avatarUploading, setAvatarUploading] = useState(false);
  const [avatarError, setAvatarError] = useState('');
  const [avatarPreview, setAvatarPreview] = useState<string | null>(null);

  // Name editing state
  const [editingName, setEditingName] = useState(false);
  const [nameValue, setNameValue] = useState('');
  const [nameSaving, setNameSaving] = useState(false);
  const nameInputRef = useRef<HTMLInputElement>(null);

  const handleStartEditName = () => {
    setNameValue(userProfile?.name || '');
    setEditingName(true);
    setTimeout(() => nameInputRef.current?.focus(), 0);
  };

  const handleSaveName = async () => {
    const trimmed = nameValue.trim();
    if (!trimmed || trimmed === userProfile?.name) {
      setEditingName(false);
      return;
    }
    setNameSaving(true);
    try {
      await updateUserName(trimmed);
    } catch (err) {
      console.error('Failed to update name:', err);
    } finally {
      setNameSaving(false);
      setEditingName(false);
    }
  };

  // Connected email accounts state
  const {
    accounts: emailAccounts,
    isLoading: accountsLoading,
    isAdding: accountAdding,
    error: accountsError,
    fetchAccounts,
    addGoogleAccount,
    addMicrosoftAccount,
    removeAccount,
  } = useEmailAccountsStore();
  const [showProviderPicker, setShowProviderPicker] = useState(false);
  const [confirmRemoveId, setConfirmRemoveId] = useState<string | null>(null);

  const handleAvatarChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    // Validate file type
    if (!file.type.startsWith('image/')) {
      setAvatarError('Please select an image file');
      return;
    }

    // Validate file size (5MB max)
    if (file.size > 5 * 1024 * 1024) {
      setAvatarError('Image must be less than 5MB');
      return;
    }

    // Show immediate preview
    const previewUrl = URL.createObjectURL(file);
    setAvatarPreview(previewUrl);

    setAvatarError('');
    setAvatarUploading(true);

    try {
      console.log('Starting avatar upload...', { name: file.name, type: file.type, size: file.size });
      const avatarUrl = await uploadAvatar(file);
      console.log('Avatar upload complete:', avatarUrl);
      updateAvatarUrl(avatarUrl);
      setAvatarPreview(null); // Clear preview, use actual URL
    } catch (err) {
      console.error('Avatar upload failed:', err);
      setAvatarError(err instanceof Error ? err.message : 'Failed to upload avatar');
      setAvatarPreview(null); // Clear preview on error
    } finally {
      setAvatarUploading(false);
      // Reset file input
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  const handleRemoveAvatar = async () => {
    setAvatarError('');
    setAvatarUploading(true);

    try {
      await deleteAvatar();
      updateAvatarUrl(null);
    } catch (err) {
      setAvatarError(err instanceof Error ? err.message : 'Failed to remove avatar');
    } finally {
      setAvatarUploading(false);
    }
  };

  // Fetch connected email accounts
  useEffect(() => {
    if (isAuthenticated) {
      fetchAccounts();
    }
  }, [isAuthenticated, fetchAccounts]);

  const handleAddProvider = async (provider: 'google' | 'microsoft') => {
    setShowProviderPicker(false);
    if (provider === 'google') {
      await addGoogleAccount();
    } else {
      await addMicrosoftAccount();
    }
  };

  const handleConfirmRemove = async () => {
    if (confirmRemoveId) {
      await removeAccount(confirmRemoveId);
      setConfirmRemoveId(null);
    }
  };

  const handleGoogleSignIn = async () => {
    setError('');
    setAuthLoadingProvider('google');
    try {
      await signInWithGoogle();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Google sign in failed');
    } finally {
      setAuthLoadingProvider(null);
    }
  };

  const handleMicrosoftSignIn = async () => {
    setError('');
    setAuthLoadingProvider('microsoft');
    try {
      await signInWithMicrosoft();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Microsoft sign in failed');
    } finally {
      setAuthLoadingProvider(null);
    }
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} title="User Settings" size="lg">
      <div className="space-y-8 max-h-[70vh] overflow-y-auto">
        {/* Profile Section */}
        {isAuthenticated && (
          <div>
            <h2 className="text-sm font-medium text-text-body mb-4">Profile</h2>

            <div className="group flex items-center justify-between">
              <div className="flex items-center gap-3 min-w-0">
                {/* Avatar */}
                <div className="relative group shrink-0">
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept="image/jpeg,image/png,image/gif,image/webp"
                    onChange={handleAvatarChange}
                    className="hidden"
                  />

                  {avatarPreview || (userProfile?.avatar_url && userProfile.avatar_url.length > 0) ? (
                    <img
                      src={avatarPreview || userProfile?.avatar_url}
                      alt="Profile"
                      className="w-8 h-8 rounded-full object-cover"
                    />
                  ) : (
                    <div
                      className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold text-white"
                      style={{ background: avatarGradient(userProfile?.name || user?.email || '?') }}
                    >
                      {user?.email?.charAt(0).toUpperCase() || '?'}
                    </div>
                  )}

                  <button
                    onClick={() => fileInputRef.current?.click()}
                    disabled={avatarUploading}
                    className="absolute inset-0 flex items-center justify-center bg-black/50 rounded-full opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer disabled:cursor-not-allowed"
                  >
                    <CameraIcon className="w-3.5 h-3.5 text-white" />
                  </button>
                </div>

                {/* Name + Email */}
                <div className="min-w-0">
                  {editingName ? (
                    <input
                      ref={nameInputRef}
                      value={nameValue}
                      onChange={(e) => setNameValue(e.target.value)}
                      onBlur={handleSaveName}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') handleSaveName();
                        if (e.key === 'Escape') setEditingName(false);
                      }}
                      disabled={nameSaving}
                      className="text-sm text-text-body font-medium bg-white border border-border-gray rounded px-1.5 py-0.5 outline-none focus:border-text-tertiary w-full"
                    />
                  ) : (
                    <p
                      className="text-sm text-text-body font-medium truncate cursor-pointer hover:underline"
                      onClick={handleStartEditName}
                    >
                      {userProfile?.name || 'Add your name'}
                    </p>
                  )}
                  <p className="text-xs text-text-secondary truncate">{user?.email}</p>
                </div>
              </div>

              {/* Remove avatar button */}
              {userProfile?.avatar_url && userProfile.avatar_url.length > 0 && !avatarUploading && (
                <button
                  onClick={handleRemoveAvatar}
                  className="px-3 py-1 text-xs text-red-400 hover:text-red-300 hover:bg-red-500/10 rounded transition-colors shrink-0 ml-2 opacity-0 group-hover:opacity-100"
                >
                  Remove photo
                </button>
              )}
            </div>

            {avatarError && (
              <p className="mt-2 text-xs text-red-400">{avatarError}</p>
            )}
          </div>
        )}

        {/* Account Section */}
        <div>
          <h2 className="text-sm font-medium text-text-body mb-4">Account</h2>

          {isAuthenticated ? (
            <div className="group flex items-center justify-between">
              <div className="min-w-0">
                <p className="text-sm text-text-body truncate">{user?.email}</p>
                <p className="text-xs text-text-secondary">Signed in</p>
              </div>
              <button
                onClick={signOut}
                className="px-3 py-1 text-xs text-red-400 hover:text-red-300 hover:bg-red-500/10 rounded transition-colors shrink-0 ml-2 opacity-0 group-hover:opacity-100"
              >
                Sign out
              </button>
            </div>
          ) : (
            <div className="space-y-4">
              {error && (
                <div className="p-3 bg-red-500/10 border border-red-500/20 rounded-lg text-sm text-red-400">
                  {error}
                </div>
              )}

              <div className="flex flex-wrap gap-3">
                <button
                  onClick={handleGoogleSignIn}
                  disabled={authLoadingProvider !== null}
                  className="inline-flex items-center h-10 rounded bg-white border border-[#747775] hover:bg-[#f8faff] hover:border-[#1f1f1f] active:bg-[#f0f4ff] transition-colors disabled:opacity-50"
                  style={{
                    fontFamily: "'Roboto', Arial, sans-serif",
                    paddingLeft: 12,
                    paddingRight: 12,
                  }}
                >
                  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" style={{ flexShrink: 0 }}>
                    <path d="M19.6 10.23c0-.68-.06-1.36-.17-2.02H10v3.82h5.38a4.6 4.6 0 0 1-2 3.02v2.5h3.24c1.89-1.74 2.98-4.3 2.98-7.32Z" fill="#4285F4"/>
                    <path d="M10 20c2.7 0 4.96-.89 6.62-2.42l-3.24-2.5c-.9.6-2.04.95-3.38.95-2.6 0-4.8-1.76-5.58-4.12H1.07v2.58A9.99 9.99 0 0 0 10 20Z" fill="#34A853"/>
                    <path d="M4.42 11.91A6.01 6.01 0 0 1 4.1 10c0-.66.11-1.3.32-1.91V5.51H1.07A9.99 9.99 0 0 0 0 10c0 1.61.39 3.14 1.07 4.49l3.35-2.58Z" fill="#FBBC05"/>
                    <path d="M10 3.98c1.47 0 2.78.5 3.82 1.5l2.86-2.86C14.96.99 12.7 0 10 0A9.99 9.99 0 0 0 1.07 5.51l3.35 2.58C5.2 5.74 7.4 3.98 10 3.98Z" fill="#EA4335"/>
                  </svg>
                  <span
                    style={{
                      marginLeft: 10,
                      fontSize: 14,
                      fontWeight: 500,
                      lineHeight: '20px',
                      color: '#1f1f1f',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {authLoadingProvider === 'google' ? 'Signing in...' : 'Sign in with Google'}
                  </span>
                </button>

                <button
                  onClick={handleMicrosoftSignIn}
                  disabled={authLoadingProvider !== null}
                  className="inline-flex items-center h-10 rounded bg-white border border-[#747775] hover:bg-[#f8faff] hover:border-[#1f1f1f] active:bg-[#f0f4ff] transition-colors disabled:opacity-50"
                  style={{
                    fontFamily: "'Roboto', Arial, sans-serif",
                    paddingLeft: 12,
                    paddingRight: 12,
                  }}
                >
                  <svg width="20" height="20" viewBox="0 0 21 21" fill="none" style={{ flexShrink: 0 }}>
                    <rect x="1" y="1" width="9" height="9" fill="#F25022"/>
                    <rect x="11" y="1" width="9" height="9" fill="#7FBA00"/>
                    <rect x="1" y="11" width="9" height="9" fill="#00A4EF"/>
                    <rect x="11" y="11" width="9" height="9" fill="#FFB900"/>
                  </svg>
                  <span
                    style={{
                      marginLeft: 10,
                      fontSize: 14,
                      fontWeight: 500,
                      lineHeight: '20px',
                      color: '#1f1f1f',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {authLoadingProvider === 'microsoft' ? 'Signing in...' : 'Sign in with Microsoft'}
                  </span>
                </button>
              </div>
            </div>
          )}
        </div>

        {/* Connected Email Accounts Section */}
        {isAuthenticated && (
          <div>
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-sm font-medium text-text-body">
                Connected Email Accounts
                <span className="text-text-secondary font-normal ml-2">
                  ({emailAccounts.length}/5)
                </span>
              </h2>
            </div>

            {accountsError && (
              <div className="p-3 mb-3 bg-red-500/10 border border-red-500/20 rounded-lg text-sm text-red-400">
                {accountsError}
              </div>
            )}

            {accountsLoading ? (
              <p className="text-sm text-text-secondary">Loading accounts...</p>
            ) : emailAccounts.length === 0 ? (
              <p className="text-sm text-text-secondary mb-3">No email accounts connected yet.</p>
            ) : (
              <div className="space-y-6 mb-4"> {/* Increased vertical spacing: space-y-6 mb-6 */}
                {emailAccounts.map((account) => (
                  <div
                    key={account.id}
                    className="group flex items-center justify-between" // Added vertical padding for extra spacing
                  >
                    <div className="flex items-center gap-4 min-w-0"> {/* Increased horizontal gap */}
                      {/* Avatar */}
                      {account.provider_avatar ? (
                        <img
                          src={account.provider_avatar}
                          alt=""
                          className="w-8 h-8 rounded-full object-cover shrink-0"
                        />
                      ) : (
                        <div
                          className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold text-white shrink-0"
                          style={{ background: account.provider === 'google' ? '#4285F4' : '#00a4ef' }}
                        >
                          {(account.provider_name || account.provider_email).charAt(0).toUpperCase()}
                        </div>
                      )}

                      {/* Info */}
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <p className="text-sm text-text-body truncate">
                            {account.provider_name || account.provider_email}
                          </p>
                          {/* Provider badge */}
                          <span className="text-[10px] px-1.5 py-0.5 rounded bg-bg-gray-dark text-text-secondary shrink-0">
                            {account.provider === 'google' ? 'Google' : 'Microsoft'}
                          </span>
                          {account.is_primary && (
                            <span className="text-[10px] px-1.5 py-0.5 rounded bg-blue-500/15 text-blue-400 shrink-0">
                              Primary
                            </span>
                          )}
                        </div>
                        {account.provider_name && (
                          <p className="text-xs text-text-secondary truncate">{account.provider_email}</p>
                        )}
                      </div>
                    </div>

                    {/* Remove button (not for primary) */}
                    {!account.is_primary && (
                      <button
                        onClick={() => setConfirmRemoveId(account.id)}
                        className="px-3 py-1 text-xs text-red-400 hover:text-red-300 hover:bg-red-500/10 rounded transition-colors shrink-0 ml-2 opacity-0 group-hover:opacity-100"
                      >
                        Remove
                      </button>
                    )}
                  </div>
                ))}
              </div>
            )}

            {/* Add account button */}
            {emailAccounts.length < 5 && (
              <button
                onClick={() => setShowProviderPicker(true)}
                disabled={accountAdding}
                className="inline-flex items-center gap-1.5 px-4 py-2 text-sm mt-3 text-text-secondary hover:text-text-body border border-border-gray rounded-lg hover:bg-bg-gray-dark/50 transition-colors disabled:opacity-50"
              >
                <PlusIcon className="w-4 h-4" />
                {accountAdding ? 'Connecting...' : 'Add another account'}
              </button>
            )}

            {/* Provider Picker Modal */}
            <Modal
              isOpen={showProviderPicker}
              onClose={() => setShowProviderPicker(false)}
              title="Connect Email Account"
              size="sm"
            >
              <p className="text-sm text-gray-500 mb-4">Choose your email provider:</p>
              <div className="space-y-2">
                <button
                  onClick={() => handleAddProvider('google')}
                  className="w-full flex items-center gap-3 p-3 rounded-lg border border-gray-200 hover:bg-gray-50 transition-colors"
                >
                  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" className="shrink-0">
                    <path d="M19.6 10.23c0-.68-.06-1.36-.17-2.02H10v3.82h5.38a4.6 4.6 0 0 1-2 3.02v2.5h3.24c1.89-1.74 2.98-4.3 2.98-7.32Z" fill="#4285F4"/>
                    <path d="M10 20c2.7 0 4.96-.89 6.62-2.42l-3.24-2.5c-.9.6-2.04.95-3.38.95-2.6 0-4.8-1.76-5.58-4.12H1.07v2.58A9.99 9.99 0 0 0 10 20Z" fill="#34A853"/>
                    <path d="M4.42 11.91A6.01 6.01 0 0 1 4.1 10c0-.66.11-1.3.32-1.91V5.51H1.07A9.99 9.99 0 0 0 0 10c0 1.61.39 3.14 1.07 4.49l3.35-2.58Z" fill="#FBBC05"/>
                    <path d="M10 3.98c1.47 0 2.78.5 3.82 1.5l2.86-2.86C14.96.99 12.7 0 10 0A9.99 9.99 0 0 0 1.07 5.51l3.35 2.58C5.2 5.74 7.4 3.98 10 3.98Z" fill="#EA4335"/>
                  </svg>
                  <div className="text-left">
                    <p className="text-sm font-medium text-gray-900">Google</p>
                    <p className="text-xs text-gray-500">Gmail, Google Calendar</p>
                  </div>
                </button>

                <button
                  onClick={() => handleAddProvider('microsoft')}
                  className="w-full flex items-center gap-3 p-3 rounded-lg border border-gray-200 hover:bg-gray-50 transition-colors"
                >
                  <svg width="20" height="20" viewBox="0 0 21 21" fill="none" className="shrink-0">
                    <rect x="1" y="1" width="9" height="9" fill="#F25022"/>
                    <rect x="11" y="1" width="9" height="9" fill="#7FBA00"/>
                    <rect x="1" y="11" width="9" height="9" fill="#00A4EF"/>
                    <rect x="11" y="11" width="9" height="9" fill="#FFB900"/>
                  </svg>
                  <div className="text-left">
                    <p className="text-sm font-medium text-gray-900">Microsoft</p>
                    <p className="text-xs text-gray-500">Outlook, Microsoft Calendar</p>
                  </div>
                </button>
              </div>
            </Modal>

            {/* Confirm Remove Modal */}
            <Modal
              isOpen={!!confirmRemoveId}
              onClose={() => setConfirmRemoveId(null)}
              title="Remove Account"
              size="sm"
            >
              <p className="text-sm text-gray-600 mb-4">
                Are you sure you want to disconnect this email account? Emails from this account will no longer be synced.
              </p>
              <div className="flex justify-end gap-2">
                <button
                  onClick={() => setConfirmRemoveId(null)}
                  className="px-4 py-2 text-sm text-gray-600 hover:text-gray-800 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
                >
                  Cancel
                </button>
                <button
                  onClick={handleConfirmRemove}
                  className="px-4 py-2 text-sm text-white bg-red-500 hover:bg-red-600 rounded-lg transition-colors"
                >
                  Remove
                </button>
              </div>
            </Modal>
          </div>
        )}

      </div>
    </Modal>
  );
}
