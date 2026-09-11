# Workspace Agent Rules

## Tool Discovery and System Inspection
Whenever you are attempting to determine whether a tool, CLI utility, or executable is installed on the system (e.g., using `Get-Command`, `where`, `which`, or probe commands):

1. **Check with the user first**: Explain what tool you are checking for and why before running terminal inspection commands.
2. **Transparent Terminal Intent**: Ensure the user understands what the upcoming terminal entry is attempting to do.
3. **Provide Context & Alternatives**: If a specific tool is sought for efficiency (such as fast extractors or specialized CLIs), explain the rationale and let the user decide.
