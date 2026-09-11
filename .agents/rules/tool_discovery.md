# Tool Discovery and System Inspection Rule

Whenever you are attempting to determine whether a tool, CLI utility, or executable is installed on the system (e.g., inspecting via `Get-Command`, `which`, `where`, running `--version` probes for unverified binaries, or checking for archive/build utilities):

1. **Explain the Purpose First**: Before running probe commands in the terminal, state what tool you are checking for and what you intend to accomplish with it.
2. **Check with the User**: Ask the user or inform them clearly so that terminal activity is transparent and understandable.
3. **Offer Alternatives**: If applicable, inform the user why the tool is being considered over defaults (e.g., performance or convenience) and give them the choice to approve or provide an alternative.
