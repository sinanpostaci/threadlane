import { Button, Callout, Flex, Text } from '@radix-ui/themes';
import { AlertCircle, Loader2, RefreshCw } from 'lucide-react';
import { useEffect, useRef } from 'react';

import {
  ToolsPage,
  type ToolsDiscoveryItem,
} from './pages/tools.ts';
import type { ToolVersionScanState } from './registry/tool-versions.ts';
import type { RegistrySnapshot } from './registry/types.ts';

export interface ToolsPanelProps {
  registry: RegistrySnapshot | null;
  registryLoading: boolean;
  registryError: string | null;
  discoveredTools: readonly ToolsDiscoveryItem[];
  discoveryLoading: boolean;
  discoveryError: string | null;
  toolVersions: ToolVersionScanState;
  onRefreshRegistry: () => void;
  onRefreshDiscovery: () => void;
  onRecheckVersions: () => void;
  onToolInspectionChanged: (registryRevision: number) => void;
  onOpenProfiles: () => void;
}

export function ToolsPanel({
  registry,
  registryLoading,
  registryError,
  discoveredTools,
  discoveryLoading,
  discoveryError,
  toolVersions,
  onRefreshRegistry,
  onRefreshDiscovery,
  onRecheckVersions,
  onToolInspectionChanged,
  onOpenProfiles,
}: ToolsPanelProps) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const toolsPageRef = useRef<ToolsPage | null>(null);

  const callbacksRef = useRef({
    onRefreshRegistry,
    onRefreshDiscovery,
    onRecheckVersions,
    onToolInspectionChanged,
    onOpenProfiles,
  });
  callbacksRef.current = {
    onRefreshRegistry,
    onRefreshDiscovery,
    onRecheckVersions,
    onToolInspectionChanged,
    onOpenProfiles,
  };

  if (toolsPageRef.current === null) {
    toolsPageRef.current = new ToolsPage(
      () => {
        callbacksRef.current.onRefreshRegistry();
      },
      {
        onOpenProfiles: () => {
          callbacksRef.current.onOpenProfiles();
        },
        onRefreshDiscovery: () => {
          callbacksRef.current.onRefreshDiscovery();
        },
      }
    );
  }

  useEffect(() => {
    const host = hostRef.current;
    const toolsPage = toolsPageRef.current;
    if (!host || !toolsPage || !registry) {
      return;
    }

    const renderedRoot = toolsPage.render(registry, {
      tools: discoveredTools,
      loading: discoveryLoading,
      error: discoveryError,
      versions: toolVersions,
      onRefreshDiscovery: () => {
        callbacksRef.current.onRefreshDiscovery();
      },
      onRecheckVersions: () => {
        callbacksRef.current.onRecheckVersions();
      },
      onToolInspectionChanged: (rev) => {
        callbacksRef.current.onToolInspectionChanged(rev);
      },
      onOpenProfiles: () => {
        callbacksRef.current.onOpenProfiles();
      },
    });

    if (host.firstChild !== renderedRoot) {
      host.replaceChildren(renderedRoot);
    }
  }, [
    registry,
    discoveredTools,
    discoveryLoading,
    discoveryError,
    toolVersions,
  ]);

  useEffect(() => {
    return () => {
      toolsPageRef.current?.dispose();
      toolsPageRef.current = null;
    };
  }, []);

  if (registry === null && registryLoading) {
    return (
      <div className="tl-panel-scroll">
        <div className="tl-panel-inner">
          <div className="tl-state-card" role="status" aria-live="polite">
            <Loader2 size={20} className="tl-spin" aria-hidden="true" />
            <Text size="2" weight="medium">
              Loading tool and model registry…
            </Text>
          </div>
        </div>
      </div>
    );
  }

  if (registry === null) {
    return (
      <div className="tl-panel-scroll">
        <div className="tl-panel-inner">
          <div className="tl-state-card" role="alert">
            <AlertCircle size={20} aria-hidden="true" />
            <Text size="2" weight="medium">
              Tool registry unavailable
            </Text>
            <Text size="2" color="gray">
              {registryError ??
                'Unable to read the local tool and model registry.'}
            </Text>
            <Button
              size="2"
              variant="soft"
              color="gray"
              onClick={onRefreshRegistry}
            >
              <RefreshCw size={15} aria-hidden="true" />
              Retry registry read
            </Button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="tl-panel-scroll">
      <div className="tl-panel-inner">
        {registryError ? (
          <Callout.Root
            color="amber"
            variant="surface"
            size="1"
            role="alert"
            style={{ marginBottom: 14 }}
          >
            <Callout.Icon>
              <AlertCircle size={15} />
            </Callout.Icon>
            <Callout.Text>
              <Flex align="center" justify="between" gap="3" wrap="wrap">
                <span>
                  Registry refresh warning: {registryError} Showing last loaded
                  revision ({registry.revision}).
                </span>
                <Button
                  size="1"
                  variant="soft"
                  color="gray"
                  onClick={onRefreshRegistry}
                >
                  <RefreshCw size={13} aria-hidden="true" />
                  Retry
                </Button>
              </Flex>
            </Callout.Text>
          </Callout.Root>
        ) : null}

        <div ref={hostRef} className="tl-tools-host" />
      </div>
    </div>
  );
}
