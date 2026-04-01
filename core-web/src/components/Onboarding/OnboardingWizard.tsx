import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { AnimatePresence } from "motion/react";
import { useAuthStore } from "../../stores/authStore";
import { useWorkspaceStore } from "../../stores/workspaceStore";
import { createWorkspaceInvitation } from "../../api/client";
import GetStartedStep from "./steps/GetStartedStep";
import WorkspaceNameStep from "./steps/WorkspaceNameStep";
import ProfileStep from "./steps/ProfileStep";
import InviteStep from "./steps/InviteStep";
import CreatingStep from "./steps/CreatingStep";
import OnboardingSidebarPreview from "./OnboardingSidebarPreview";

type Step =
  | "get-started"
  | "workspace-name"
  | "profile"
  | "invite"
  | "creating";

interface OnboardingData {
  workspaceName: string;
  userName: string;
  avatarUrl: string | null;
  inviteEmails: string[];
}

export default function OnboardingWizard() {
  const navigate = useNavigate();
  const userProfile = useAuthStore((s) => s.userProfile);
  const [step, setStep] = useState<Step>("get-started");
  const [creatingStatus, setCreatingStatus] = useState(
    "Creating your workspace"
  );

  const [data, setData] = useState<OnboardingData>(() => ({
    workspaceName: "",
    userName: userProfile?.name || "",
    avatarUrl: userProfile?.avatar_url || null,
    inviteEmails: [],
  }));

  const updateData = useCallback(
    (updates: Partial<OnboardingData>) => {
      setData((prev) => ({ ...prev, ...updates }));
    },
    []
  );

  const handleComplete = useCallback(
    async (finalData: OnboardingData) => {
      setStep("creating");

      const { addWorkspace, setActiveWorkspace } =
        useWorkspaceStore.getState();
      const { updateUserName, completeOnboarding } =
        useAuthStore.getState();

      let newWorkspace: Awaited<ReturnType<typeof addWorkspace>> | null = null;

      // 1. Create new workspace
      if (finalData.workspaceName.trim()) {
        setCreatingStatus("Creating your workspace");
        try {
          newWorkspace = await addWorkspace(finalData.workspaceName.trim());
        } catch (err) {
          console.error("Onboarding: failed to create workspace:", err);
        }
      }

      // 2. Update user name (independent)
      if (finalData.userName.trim()) {
        setCreatingStatus("Setting up your profile");
        try {
          await updateUserName(finalData.userName.trim());
        } catch (err) {
          console.error("Onboarding: failed to update user name:", err);
        }
      }

      // 3. Send invitations to new workspace (independent)
      if (finalData.inviteEmails.length > 0 && newWorkspace) {
        setCreatingStatus("Sending invitations");
        for (const email of finalData.inviteEmails) {
          try {
            await createWorkspaceInvitation(newWorkspace.id, email, "member");
          } catch {
            // Skip failed invites silently
          }
        }
      }

      // 4. Mark onboarding complete (critical — always attempt)
      setCreatingStatus("Almost done");
      try {
        await completeOnboarding();
      } catch (err) {
        console.error("Onboarding: failed to mark complete:", err);
      }

      // 5. Switch to the new workspace and navigate to notes
      await new Promise((r) => setTimeout(r, 600));
      if (newWorkspace) {
        setActiveWorkspace(newWorkspace.id);
        const filesPath = newWorkspace.welcomeNoteId
          ? `/workspace/${newWorkspace.id}/files/${newWorkspace.welcomeNoteId}`
          : `/workspace/${newWorkspace.id}/files`;
        navigate(filesPath, { replace: true });
      } else {
        navigate("/files", { replace: true });
      }
    },
    [navigate]
  );

  const isSplitStep = step === "workspace-name" || step === "profile" || step === "invite";

  // Full-width centered layout for splash / creating
  if (!isSplitStep) {
    return (
      <div className="h-screen w-screen bg-white flex items-center justify-center overflow-hidden">
        <div className="w-full max-w-[480px] px-8">
          <AnimatePresence mode="wait">
            {step === "get-started" && (
              <GetStartedStep
                key="get-started"
                onNext={() => setStep("workspace-name")}
              />
            )}
            {step === "creating" && (
              <CreatingStep key="creating" status={creatingStatus} />
            )}
          </AnimatePresence>
        </div>
      </div>
    );
  }

  // Split layout: form left, preview right
  return (
    <div className="h-screen w-screen flex overflow-hidden">
      {/* Left: white form area */}
      <div className="w-1/2 bg-white flex items-center justify-center px-16">
        <div className="w-full max-w-[440px]">
          <AnimatePresence mode="wait">
            {step === "workspace-name" && (
              <WorkspaceNameStep
                key="workspace-name"
                value={data.workspaceName}
                onChange={(name) => updateData({ workspaceName: name })}
                onNext={() => setStep("profile")}
                onBack={() => setStep("get-started")}
              />
            )}

            {step === "profile" && (
              <ProfileStep
                key="profile"
                name={data.userName}
                avatarUrl={data.avatarUrl}
                onNameChange={(name) => updateData({ userName: name })}
                onAvatarChange={(url) =>
                  updateData({ avatarUrl: url })
                }
                onNext={() => setStep("invite")}
                onBack={() => setStep("workspace-name")}
              />
            )}

            {step === "invite" && (
              <InviteStep
                key="invite"
                emails={data.inviteEmails}
                onEmailsChange={(emails) =>
                  updateData({ inviteEmails: emails })
                }
                onNext={() => handleComplete(data)}
                onSkip={() =>
                  handleComplete({ ...data, inviteEmails: [] })
                }
                onBack={() => setStep("profile")}
              />
            )}
          </AnimatePresence>
        </div>
      </div>

      {/* Right: colored area with preview */}
      <div className="w-1/2 bg-bg-panel hidden md:flex items-center justify-center px-16">
        <OnboardingSidebarPreview
          workspaceName={data.workspaceName || "Your workspace"}
          userName={data.userName || "Your name"}
          avatarUrl={data.avatarUrl}
        />
      </div>
    </div>
  );
}
