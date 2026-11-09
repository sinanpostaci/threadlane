export function isTraySurfaceRoute(search?: string): boolean {
  const query =
    search ?? (typeof window !== 'undefined' ? window.location.search : '');
  const params = new URLSearchParams(query);
  return params.get('surface') === 'tray';
}

export async function bootstrapDesktopSurface(
  mount: HTMLElement,
  search?: string
): Promise<void> {
  if (isTraySurfaceRoute(search)) {
    const { mountTrayPopover } = await import('./tray.tsx');
    mountTrayPopover(mount);
    return;
  }

  const { mountDesktopApp } = await import('./app.tsx');
  mountDesktopApp(mount);
}

if (typeof document !== 'undefined') {
  const mount = document.getElementById('app');
  if (mount) {
    void bootstrapDesktopSurface(mount);
  }
}
