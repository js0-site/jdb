cargo remove --dev tokio

fd --type file --hidden --exclude .git -x sd tokio compio "{}"

cargo add --dev compio -F macros

echo -e "\n禁用tokio，基于compio单线程异步生态开发" >>AGENTS.md
