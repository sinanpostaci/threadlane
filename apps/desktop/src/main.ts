const DESKTOP_SURFACE_QUERY_PARAM = 'surface';
const TRAY_SURFACE_QUERY_VALUE = 'tray';

export function isTraySurfaceRoute(search?: string): boolean {
  const query =
    search ?? (typeof window !== 'undefined' ? window.location.search : '');
  const params = new URLSearchParams(query);
  return params.get(DESKTOP_SURFACE_QUERY_PARAM) === TRAY_SURFACE_QUERY_VALUE;
}

export async function bootstrapDesktopSurface(
  mount: HTMLElement,
  search?: string
): Promise<void> {
  if (isTraySurfaceRoute(search)) {
    mount.dataset.desktopSurface = TRAY_SURFACE_QUERY_VALUE;
    const { mountTrayPopover } = await import('./tray.tsx');
    mountTrayPopover(mount);
    return;
  }

  mount.dataset.desktopSurface = 'main';
  const { mountDesktopApp } = await import('./app.tsx');
  mountDesktopApp(mount);
}

if (typeof document !== 'undefined') {
  const mount = document.getElementById('app');
  if (mount) {
    void bootstrapDesktopSurface(mount);
  }
}
