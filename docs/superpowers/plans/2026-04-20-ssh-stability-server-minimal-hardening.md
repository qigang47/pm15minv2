# SSH Stability Server Minimal Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 以最小改动方式提升 `ht66` 服务器 SSH 新连接和长连接的稳定性，不改变端口、认证模式和密钥体系。

**Architecture:** 先读取服务器当前 SSH 配置与提权条件，只在 `/etc/ssh/sshd_config` 中补充保活、登录握手和并发上限参数；变更前备份，重启前校验配置，重启后用新连接验证可登录。

**Tech Stack:** OpenSSH server, Ubuntu service management, shell commands

---

### Task 1: Inspect Current SSH State

**Files:**
- Read: `/etc/ssh/sshd_config`

- [ ] **Step 1: Read current SSH settings**

Run: `ssh ht66 'grep -nE "^(UseDNS|TCPKeepAlive|ClientAliveInterval|ClientAliveCountMax|LoginGraceTime|MaxStartups|MaxSessions)" /etc/ssh/sshd_config || true'`
Expected: print existing matching lines or nothing if unset.

- [ ] **Step 2: Check sudo mode**

Run: `ssh ht66 'sudo -n true && echo sudo_nopass=yes || echo sudo_nopass=no'`
Expected: one-line sudo availability result.

### Task 2: Apply Minimal SSH Hardening

**Files:**
- Modify: `/etc/ssh/sshd_config`
- Backup: `/etc/ssh/sshd_config.codex.bak`

- [ ] **Step 1: Back up current config**

Run: `ssh ht66 'sudo cp /etc/ssh/sshd_config /etc/ssh/sshd_config.codex.bak'`
Expected: backup file created with exit code 0.

- [ ] **Step 2: Write minimal stable settings**

Settings to enforce:

```text
UseDNS no
TCPKeepAlive yes
ClientAliveInterval 30
ClientAliveCountMax 6
LoginGraceTime 30
MaxStartups 50:30:200
MaxSessions 64
```

- [ ] **Step 3: Validate SSH config before restart**

Run: `ssh ht66 'sudo sshd -t'`
Expected: exit code 0 and no syntax error output.

### Task 3: Restart And Verify

**Files:**
- Read: `/etc/ssh/sshd_config`

- [ ] **Step 1: Restart SSH service**

Run one of:

```bash
ssh ht66 'sudo systemctl restart ssh'
ssh ht66 'sudo service ssh restart'
```

Expected: service restart succeeds.

- [ ] **Step 2: Verify fresh login works**

Run: `ssh -o BatchMode=yes -o ConnectTimeout=10 ht66 'echo ok && hostname'`
Expected: prints `ok` and host name quickly.

- [ ] **Step 3: Re-read effective config surface**

Run: `ssh ht66 'grep -nE "^(UseDNS|TCPKeepAlive|ClientAliveInterval|ClientAliveCountMax|LoginGraceTime|MaxStartups|MaxSessions)" /etc/ssh/sshd_config'`
Expected: all target lines present with intended values.
