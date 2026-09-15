import { useCallback, useEffect, useState } from 'react'

export type Theme = 'light' | 'dark'

const STORAGE_KEY = 'hospital-console-theme'

function systemPrefersDark(): boolean {
  return window.matchMedia?.('(prefers-color-scheme: dark)').matches ?? false
}

function readStored(): Theme | null {
  try {
    const v = localStorage.getItem(STORAGE_KEY)
    return v === 'light' || v === 'dark' ? v : null
  } catch {
    // localStorage unavailable (private browsing, etc.) — fall back to system.
    return null
  }
}

function apply(theme: Theme) {
  document.documentElement.classList.toggle('dark', theme === 'dark')
}

/** Class-based dark mode (see the `@custom-variant dark` in index.css):
 * defaults to the OS preference, overridable per-browser via a toggle
 * persisted in localStorage. */
export function useTheme() {
  const [theme, setThemeState] = useState<Theme>(() => {
    const stored = readStored()
    const initial = stored ?? (systemPrefersDark() ? 'dark' : 'light')
    apply(initial)
    return initial
  })

  useEffect(() => {
    // Only follow the OS setting live when the user hasn't overridden it.
    if (readStored()) return
    const media = window.matchMedia('(prefers-color-scheme: dark)')
    const listener = (e: MediaQueryListEvent) => {
      const next = e.matches ? 'dark' : 'light'
      apply(next)
      setThemeState(next)
    }
    media.addEventListener('change', listener)
    return () => media.removeEventListener('change', listener)
  }, [])

  const setTheme = useCallback((next: Theme) => {
    apply(next)
    setThemeState(next)
    try {
      localStorage.setItem(STORAGE_KEY, next)
    } catch {
      // Ignore — theme just won't persist across reloads.
    }
  }, [])

  const toggle = useCallback(() => {
    setTheme(theme === 'dark' ? 'light' : 'dark')
  }, [theme, setTheme])

  return { theme, setTheme, toggle }
}
