cargo remove --dev compio

fd --type file --hidden --exclude .git -x sd compio compio "{}"

cargo add --dev compio -F macros
