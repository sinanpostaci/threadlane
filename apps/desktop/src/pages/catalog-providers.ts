export type CatalogProvider = 'all' | 'opencode-go' | 'opencode' | 'other';

export function catalogProvider(value: string): CatalogProvider {
  return value === 'opencode-go' || value === 'opencode' || value === 'other'
    ? value
    : 'all';
}

export function modelCatalogProvider(argument: string): Exclude<CatalogProvider, 'all'> {
  const separator = argument.indexOf('/');
  if (separator < 1 || separator === argument.length - 1) return 'other';
  const provider = argument.slice(0, separator);
  return provider === 'opencode-go' || provider === 'opencode' ? provider : 'other';
}

export function matchesCatalogProvider(argument: string, provider: CatalogProvider): boolean {
  return provider === 'all' || modelCatalogProvider(argument) === provider;
}

export function catalogProviderOptions(models: readonly { model_arg: string }[]): {
  value: CatalogProvider;
  label: string;
}[] {
  const counts = { 'opencode-go': 0, opencode: 0, other: 0 };
  for (const model of models) counts[modelCatalogProvider(model.model_arg)]++;
  const options: { value: CatalogProvider; label: string }[] = [
    { value: 'all', label: `All providers (${models.length})` },
    { value: 'opencode-go', label: `OpenCode Go (${counts['opencode-go']})` },
    { value: 'opencode', label: `OpenCode API (${counts.opencode})` },
  ];
  if (counts.other > 0) options.push({ value: 'other', label: `Other providers (${counts.other})` });
  return options;
}
