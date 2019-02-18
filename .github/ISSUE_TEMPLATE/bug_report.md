---
name: Bug report
about: Report a bug - something that is documented to work but does not
title: ''
labels: ''
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is. A bug is something that is documented to work, but doesn't work or doesn't work as documented.

*Do not ask for support for new PostgreSQL versions here*. See [feature requests]().

**To Reproduce**
Steps to reproduce the behavior. Supply a *complete* list of steps from a *bare install of PostgreSQL-9.4 BDR* including your schema creation, dummy data, all exact commands, etc.

If you do not have a reproducible test case, you need to be willing to follow instructions to debug the issue and you will need to have a basic grasp of using `gdb` and other development tools.

For general help requests or issues that have not yet been narrowed to a specific bug, [please post on the mailing list instead](https://groups.google.com/a/2ndquadrant.com/forum/?nomobile=true#!forum/bdr-list). Mail is *moderated* so be patient, your post will not appear immediately, but it will within a couple of days at worst.

**Expected behavior**
A clear and concise description of what you expected to happen.

**Logs**
Include relevant excerpts from your PostgreSQL logs. Also show relevant `bdr.bdr_nodes`, `bdr.bdr_connections` etc contents from the node(s). Label everything clearly and consistently with the node it came from.

**Additional context**
Add any other context about the problem here.
