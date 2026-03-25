import { useParams } from 'react-router-dom';
import { useWorkspaceStore } from '../../stores/workspaceStore';
import EmailsCard from './components/EmailsCard';
import QuickChatCard from './components/QuickChatCard';
import ProjectsCard from './components/ProjectsCard';
import CalendarCard from './components/CalendarCard';
import AgentsCard from './components/AgentsCard';

export default function DashboardView() {
  const { workspaceId } = useParams<{ workspaceId: string }>();
  const workspaces = useWorkspaceStore((state) => state.workspaces);
  const workspace = workspaces.find((w) => w.id === workspaceId);

  if (!workspace) {
    return (
      <div className="flex-1 flex items-center justify-center bg-background-primary">
        <p className="text-text-tertiary">Workspace not found</p>
      </div>
    );
  }

  return (
    <div className="flex-1 flex h-full overflow-hidden">
      <div className="flex-1 flex min-w-0 overflow-hidden bg-white relative">
          <div className="flex-1 min-w-0 overflow-auto p-4">
            {/* Bento Grid Layout */}
            <div className="grid grid-cols-6 grid-rows-[2fr_3fr] gap-3 w-full h-full">
              {/* Top Row - 2 cards (taller, 50% each) */}
              <div className="col-span-3">
                <EmailsCard />
              </div>
              <div className="col-span-3">
                <QuickChatCard />
              </div>

              {/* Bottom Row - 3 cards (shorter, 33% each) */}
              <div className="col-span-2">
                <ProjectsCard />
              </div>
              <div className="col-span-2">
                <CalendarCard />
              </div>
              <div className="col-span-2">
                <AgentsCard />
              </div>
            </div>
          </div>
      </div>
    </div>
  );
}
