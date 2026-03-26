-- ============================================================
-- Bumblebee Bot — Полная схема БД
-- Применить: psql $DATABASE_URL -f db/init.sql
-- ============================================================

-- 1. Пользователи платформы (владельцы ботов)
CREATE TABLE IF NOT EXISTS platform_users (
    user_id         BIGINT PRIMARY KEY,
    username        VARCHAR(64),
    first_name      VARCHAR(128),
    language        VARCHAR(8) DEFAULT 'ru',   -- ru | en
    tariff          VARCHAR(16) DEFAULT 'free', -- free | start | pro | business
    tariff_until    TIMESTAMPTZ,
    trial_used      BOOLEAN DEFAULT false,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- 2. Дочерние боты пользователей (созданные через BotFather)
CREATE TABLE IF NOT EXISTS child_bots (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    bot_id          BIGINT NOT NULL,              -- TG id бота
    bot_username    VARCHAR(64) NOT NULL,         -- @username без @
    bot_name        VARCHAR(128) NOT NULL,        -- display name
    token_encrypted TEXT NOT NULL,               -- Fernet-зашифрованный токен
    verify_only     BOOLEAN DEFAULT false,        -- только владелец может добавлять площадки
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(owner_id, bot_id)
);

-- 3. Подключённые площадки (каналы и группы)
CREATE TABLE IF NOT EXISTS bot_chats (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    child_bot_id    INTEGER REFERENCES child_bots(id) ON DELETE CASCADE,
    chat_id         BIGINT NOT NULL,
    chat_title      VARCHAR(256),
    chat_type       VARCHAR(16),               -- channel | supergroup | group
    is_active       BOOLEAN DEFAULT true,
    captcha_enabled BOOLEAN DEFAULT false,
    captcha_type    VARCHAR(16) DEFAULT 'button',
    captcha_text    TEXT,
    captcha_timer   INTEGER DEFAULT 60,
    captcha_delete  BOOLEAN DEFAULT false,
    welcome_text    TEXT,
    welcome_media   TEXT,
    farewell_text   TEXT,
    autoaccept      BOOLEAN DEFAULT false,
    autoaccept_delay INTEGER DEFAULT 0,
    filter_rtl      BOOLEAN DEFAULT false,
    filter_hieroglyph BOOLEAN DEFAULT false,
    filter_no_photo  BOOLEAN DEFAULT false,
    reaction_emojis TEXT[],
    feedback_enabled BOOLEAN DEFAULT false,
    feedback_target  VARCHAR(16) DEFAULT 'owner',
    timezone        VARCHAR(64) DEFAULT 'UTC',
    added_at        TIMESTAMPTZ DEFAULT now(),
    media_bottom    BOOLEAN DEFAULT false,
    edit_welcome_mid INTEGER,
    edit_farewell_mid INTEGER,
    UNIQUE(owner_id, chat_id)
);


-- 3. Языковые фильтры для площадки
CREATE TABLE IF NOT EXISTS language_filters (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    chat_id         BIGINT NOT NULL,
    language_code   VARCHAR(8) NOT NULL,       -- ru | en | uk | ...
    UNIQUE(owner_id, chat_id, language_code)
);

-- 4. Участники площадок (для рассылки и аналитики)
CREATE TABLE IF NOT EXISTS bot_users (
    id              BIGSERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    chat_id         BIGINT NOT NULL,
    user_id         BIGINT NOT NULL,
    username        VARCHAR(64),
    first_name      VARCHAR(128),
    language_code   VARCHAR(8),
    is_premium      BOOLEAN DEFAULT false,
    has_rtl         BOOLEAN DEFAULT false,
    has_hieroglyph  BOOLEAN DEFAULT false,
    bot_activated   BOOLEAN DEFAULT false,     -- открыл диалог с ботом = можно слать рассылку
    is_active       BOOLEAN DEFAULT true,      -- false = отписался или забанен
    joined_via_link_id INTEGER,                -- REFERENCES invite_links(id) — для счётчика отписок
    joined_at       TIMESTAMPTZ DEFAULT now(),
    left_at         TIMESTAMPTZ,
    UNIQUE(owner_id, chat_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_bot_users_owner_chat ON bot_users(owner_id, chat_id) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_bot_users_activated  ON bot_users(owner_id, bot_activated) WHERE bot_activated = true;

-- 5. Чёрный список
CREATE TABLE IF NOT EXISTS blacklist (
    id              BIGSERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    user_id         BIGINT,
    username        VARCHAR(64),
    reason          TEXT,
    added_at        TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT bl_has_identifier CHECK (user_id IS NOT NULL OR username IS NOT NULL)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bl_uid_global   ON blacklist(owner_id, user_id)   WHERE user_id IS NOT NULL AND child_bot_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_bl_uname_global ON blacklist(owner_id, lower(username)) WHERE username IS NOT NULL AND child_bot_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_bl_uid_bot      ON blacklist(owner_id, child_bot_id, user_id)   WHERE user_id IS NOT NULL AND child_bot_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_bl_uname_bot    ON blacklist(owner_id, child_bot_id, lower(username)) WHERE username IS NOT NULL AND child_bot_id IS NOT NULL;

-- 6. Рассылки
CREATE TABLE IF NOT EXISTS mailings (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    chat_id         BIGINT,                    -- NULL = bot-level (all chats)
    child_bot_id    INTEGER REFERENCES child_bots(id) ON DELETE CASCADE,
    text            TEXT,
    media_file_id   TEXT,
    media_type      VARCHAR(16),               -- photo | video | document | NULL
    inline_buttons  JSONB,                     -- [{text, url}]
    status          VARCHAR(16) DEFAULT 'draft', -- draft | pending | running | done | cancelled
    sent_count      INTEGER DEFAULT 0,
    error_count     INTEGER DEFAULT 0,
    total_count     INTEGER DEFAULT 0,
    scheduled_at    TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- 7. Ссылки-приглашения
CREATE TABLE IF NOT EXISTS invite_links (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    chat_id         BIGINT NOT NULL,
    name            VARCHAR(128) NOT NULL,
    link            TEXT NOT NULL,
    link_type       VARCHAR(16) DEFAULT 'request', -- request | regular | onetime
    member_limit    INTEGER,                   -- NULL = безлимит
    budget          NUMERIC(10,2),
    budget_currency CHAR(3),
    joined          INTEGER DEFAULT 0,
    unsubscribed    INTEGER DEFAULT 0,
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- 8. Автоответчик (только для групп)
CREATE TABLE IF NOT EXISTS autoresponder_rules (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    chat_id         BIGINT NOT NULL,
    keyword         VARCHAR(256),              -- NULL = общий ответ
    response_text   TEXT NOT NULL,
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- 9. Члены команды (модераторы, администраторы)
CREATE TABLE IF NOT EXISTS team_members (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    user_id         BIGINT NOT NULL,
    username        VARCHAR(64),
    role            VARCHAR(16) DEFAULT 'moderator', -- admin | moderator
    invite_token    VARCHAR(64) UNIQUE,        -- одноразовый токен для вступления
    is_active       BOOLEAN DEFAULT true,
    added_at        TIMESTAMPTZ DEFAULT now(),
    UNIQUE(owner_id, user_id)
);

-- 10. Платежи (NOWPayments)
CREATE TABLE IF NOT EXISTS payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    tariff          VARCHAR(16) NOT NULL,      -- start | pro | business
    period          VARCHAR(8) NOT NULL,       -- month | year
    amount_usd      DECIMAL(10,2) NOT NULL,
    currency        VARCHAR(16),               -- usdttrc20 | ton | eth | btc
    np_payment_id   VARCHAR(64),
    status          VARCHAR(16) DEFAULT 'pending', -- pending | paid | expired | failed
    created_at      TIMESTAMPTZ DEFAULT now(),
    paid_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_payments_user   ON payments(user_id, status);
CREATE INDEX IF NOT EXISTS idx_payments_np     ON payments(np_payment_id) WHERE np_payment_id IS NOT NULL;

-- 11. Журнал действий (для тарифа Про+)
CREATE TABLE IF NOT EXISTS audit_log (
    id              BIGSERIAL PRIMARY KEY,
    owner_id        BIGINT NOT NULL,
    actor_id        BIGINT,                    -- кто совершил (владелец или модератор)
    target_id       BIGINT,                    -- над кем
    action          VARCHAR(64) NOT NULL,      -- ban | kick | approve | reject | bl_add
    chat_id         BIGINT,
    details         JSONB,
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_owner_created ON audit_log(owner_id, created_at DESC);

-- ════════════════════════════════════════════════════════════
-- МИГРАЦИИ (безопасны при повторном запуске)
-- ════════════════════════════════════════════════════════════

-- Добавить child_bot_id в bot_chats (если не существует)
ALTER TABLE bot_chats
    ADD COLUMN IF NOT EXISTS child_bot_id INTEGER REFERENCES child_bots(id) ON DELETE CASCADE;

-- Таблица ожидающих заявок на вступление
CREATE TABLE IF NOT EXISTS join_requests (
    id              BIGSERIAL PRIMARY KEY,
    owner_id        BIGINT NOT NULL REFERENCES platform_users(user_id) ON DELETE CASCADE,
    chat_id         BIGINT NOT NULL,
    user_id         BIGINT NOT NULL,
    username        VARCHAR(64),
    first_name      VARCHAR(128),
    status          VARCHAR(16) DEFAULT 'pending',  -- pending | approved | declined | expired
    requested_at    TIMESTAMPTZ DEFAULT now(),
    resolved_at     TIMESTAMPTZ,
    UNIQUE(owner_id, chat_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_join_req_pending
    ON join_requests(owner_id, chat_id) WHERE status = 'pending';

-- blacklist_enabled: позволяет включить/выключить проверку ЧС для бота
ALTER TABLE child_bots
    ADD COLUMN IF NOT EXISTS blacklist_enabled BOOLEAN DEFAULT TRUE;

-- child_bot_id в team_members: позволяет назначать админа к конкретному боту
ALTER TABLE team_members
    ADD COLUMN IF NOT EXISTS child_bot_id INTEGER REFERENCES child_bots(id) ON DELETE CASCADE;

-- Одноразовые токены приглашений в команду
CREATE TABLE IF NOT EXISTS team_invites (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    child_bot_id    INTEGER REFERENCES child_bots(id) ON DELETE CASCADE,
    token           VARCHAR(64) UNIQUE NOT NULL,
    role            VARCHAR(16) NOT NULL,   -- 'admin' | 'owner'
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_team_invites_token ON team_invites(token);

-- Таблица платежей за тарифы (NOWPayments)
CREATE TABLE IF NOT EXISTS payments (
    id              SERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES platform_users(user_id) ON DELETE CASCADE,
    tariff          VARCHAR(16) NOT NULL,    -- start | pro | business
    period          VARCHAR(8)  NOT NULL,    -- month | year
    amount_usd      NUMERIC(10,2) NOT NULL,
    currency        VARCHAR(16) DEFAULT 'usd',
    status          VARCHAR(16) DEFAULT 'pending',  -- pending | paid | failed | expired
    np_payment_id   TEXT,                   -- ID от NOWPayments
    created_at      TIMESTAMPTZ DEFAULT now(),
    paid_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);
ALTER TABLE payments ADD COLUMN IF NOT EXISTS applied_discount INTEGER DEFAULT 0;

ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS welcome_media_type  VARCHAR(16);   -- photo | video | animation | NULL
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS welcome_buttons      JSONB;         -- [{text, url}, ...]
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS welcome_preview      BOOLEAN DEFAULT FALSE;
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS welcome_timer        INTEGER DEFAULT 0;  -- секунды; 0 = не удалять
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS farewell_media       TEXT;          -- file_id
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS farewell_media_type  VARCHAR(16);
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS farewell_buttons     JSONB;
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS farewell_preview     BOOLEAN DEFAULT FALSE;
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS farewell_timer       INTEGER DEFAULT 0;

ALTER TABLE invite_links ADD COLUMN IF NOT EXISTS males             INTEGER DEFAULT 0;
ALTER TABLE invite_links ADD COLUMN IF NOT EXISTS females           INTEGER DEFAULT 0;
ALTER TABLE invite_links ADD COLUMN IF NOT EXISTS rtl_count         INTEGER DEFAULT 0;   -- RTL-символы в имени
ALTER TABLE invite_links ADD COLUMN IF NOT EXISTS hieroglyph_count  INTEGER DEFAULT 0;   -- Иероглифы в имени
ALTER TABLE invite_links ADD COLUMN IF NOT EXISTS premium_count     INTEGER DEFAULT 0;   -- Telegram Premium
ALTER TABLE invite_links ADD COLUMN IF NOT EXISTS countries         JSONB DEFAULT '{}';  -- {"RU": 5, "UA": 3}
ALTER TABLE invite_links ADD COLUMN IF NOT EXISTS auto_accept       VARCHAR(16) DEFAULT 'base'; -- base | on | off

-- Лимиты вступлений
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS join_limit_enabled      BOOLEAN   DEFAULT false;
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS join_limit_punishment   VARCHAR(8) DEFAULT 'kick'; -- kick | ban
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS join_limit_period_min   INTEGER   DEFAULT 1;    -- 1 | 5 | 10 | 30
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS join_limit_count        INTEGER   DEFAULT 50;   -- порог вступлений

-- Позиция медиа в рассылке: false = сверху (caption), true = снизу (отдельным сообщением)
ALTER TABLE mailings ADD COLUMN IF NOT EXISTS media_below BOOLEAN DEFAULT false;

-- Медиа (фото) для сообщения капчи: file_id фотографии
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_media TEXT;

-- Текст кнопок капчи: строка с кнопками, по одной на строку (с опциональным цветным квадратом)
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_buttons_raw TEXT;

-- Блокировка пользователей по feedback: пользователь не сможет слать сообщения владельцу
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS feedback_blocked BOOLEAN DEFAULT false;

-- ════════════════════════════════════════════════════════════
-- ВЛАДЕЛЕЦ ПРОЕКТА: вечный тариф Business (@alextgads)
-- Выполняется при каждом деплое — безопасно, идемпотентно.
-- Если пользователь ещё не зарегистрирован, UPDATE просто ничего не затронет.
-- ════════════════════════════════════════════════════════════
UPDATE platform_users
SET tariff = 'business', tariff_until = NULL
WHERE lower(username) = 'alextgads';

-- Tracking table for pinned/delete-scheduled mailing messages
CREATE TABLE IF NOT EXISTS mailing_sent_messages (
    id              BIGSERIAL PRIMARY KEY,
    mailing_id      INTEGER REFERENCES mailings(id) ON DELETE CASCADE,
    child_bot_id    INTEGER REFERENCES child_bots(id) ON DELETE SET NULL,
    tg_user_id      BIGINT NOT NULL,           -- recipient user_id (private chat)
    tg_message_id   INTEGER NOT NULL,          -- message_id returned by Telegram
    pin_until       TIMESTAMPTZ,               -- NULL = no pin; set to sent_at + 24h when pin_message=True
    delete_after    BOOLEAN DEFAULT false,     -- mirrors delete_after_send from mailing
    unpinned        BOOLEAN DEFAULT false,     -- true once unpin+delete has been executed
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_msm_pending
    ON mailing_sent_messages(pin_until, unpinned)
    WHERE pin_until IS NOT NULL AND unpinned = false;

-- Реестр уникальных участников по каждой ссылке для дедупликации статистики
CREATE TABLE IF NOT EXISTS invite_link_members (
    link_id         INTEGER   NOT NULL REFERENCES invite_links(id) ON DELETE CASCADE,
    user_id         BIGINT    NOT NULL,
    joined_counted  BOOLEAN   DEFAULT false,  -- был ли учтён в invite_links.joined
    PRIMARY KEY (link_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_ilm_link_id ON invite_link_members(link_id);

-- Общий автоответ: отвечать на любое сообщение одним текстом
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS general_reply_enabled   BOOLEAN DEFAULT false;
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS general_reply_text       TEXT;
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS general_reply_buttons    JSONB;        -- [{text, url}, ...]
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS general_reply_media      TEXT;         -- file_id
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS general_reply_media_type VARCHAR(16);  -- photo | video | document
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS general_reply_preview    BOOLEAN DEFAULT false;

-- Статистика сообщений: каждое сообщение в группе/супергруппе
CREATE TABLE IF NOT EXISTS message_events (
    id          BIGSERIAL PRIMARY KEY,
    owner_id    BIGINT NOT NULL REFERENCES platform_users(user_id) ON DELETE CASCADE,
    chat_id     BIGINT NOT NULL,
    user_id     BIGINT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_msg_events_owner_chat_date
    ON message_events(owner_id, chat_id, created_at);

-- captcha_animation
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_animation BOOLEAN DEFAULT false;
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_anim_file_id TEXT;
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_anim_type VARCHAR(16);

-- captcha_greet
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS captcha_greet BOOLEAN DEFAULT false;

ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS farewell_media_below BOOLEAN DEFAULT false;

-- ════════════════════════════════════════════════════════════
-- Глобальная Экосистема (Global Admin Ecosystem)
-- ════════════════════════════════════════════════════════════

-- Глобальные администраторы
CREATE TABLE IF NOT EXISTS global_admins (
    id              SERIAL PRIMARY KEY,
    owner_id        BIGINT REFERENCES platform_users(user_id) ON DELETE CASCADE,
    admin_id        BIGINT NOT NULL,
    admin_username  VARCHAR(64),
    added_at        TIMESTAMPTZ DEFAULT now(),
    UNIQUE(owner_id, admin_id)
);

-- Фиксация того, кто выдал бан
ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS added_by BIGINT;

-- Использование глобального ЧС для конкретного бота
ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS use_global_blacklist BOOLEAN DEFAULT false;

-- Настройка умного Авто-Бана на уровне владельца
ALTER TABLE platform_users ADD COLUMN IF NOT EXISTS global_auto_ban BOOLEAN DEFAULT false;

-- Активация бота в глобальной сети (влияет на статистику, рассылки и ЧС)
ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS in_global_network BOOLEAN DEFAULT false;

-- Журнал действий администраторов (Audit Log)
CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGSERIAL PRIMARY KEY,
    owner_id    BIGINT NOT NULL,
    user_id     BIGINT,
    action      VARCHAR(64) NOT NULL DEFAULT 'unknown',
    entity_type VARCHAR(32),
    entity_id   BIGINT,
    details     TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_log_owner ON audit_log(owner_id, created_at DESC);

-- Гарантируем наличие всех колонок audit_log (если таблица создана старой миграцией)
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS user_id     BIGINT;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS action      VARCHAR(64) NOT NULL DEFAULT 'unknown';
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS entity_type VARCHAR(32);
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS entity_id   BIGINT;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS details     TEXT;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS created_at  TIMESTAMPTZ DEFAULT now();

-- Мастер-тумблер глобального ЧС (включить/выключить блокировку для всей сети)
ALTER TABLE platform_users ADD COLUMN IF NOT EXISTS blacklist_active BOOLEAN DEFAULT true;

-- Счётчик сработавших блокировок ЧС (отклонённые заявки + кики из каналов)
ALTER TABLE platform_users ADD COLUMN IF NOT EXISTS blocked_count BIGINT DEFAULT 0;

-- Per-bot счётчик заблокированных ботом (для меню ≡ База)
ALTER TABLE child_bots ADD COLUMN IF NOT EXISTS blocked_count BIGINT DEFAULT 0;

-- Per-bot изоляция чёрного списка (NULL = глобальная запись от admin)
ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS child_bot_id INTEGER REFERENCES child_bots(id) ON DELETE CASCADE;

-- ════════════════════════════════════════════════════════════
-- Суперадминская выборка ботов (платформенная)
-- Каждый глобальный администратор формирует свою персональную "рабочую выборку" ботов
-- из ВСЕХ ботов платформы (любых пользователей).
-- Эта выборка используется как зона поражения Глобального ЧС
-- и источник данных для кросс-пользовательского экспорта аудитории.
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ga_selected_bots (
    admin_id     BIGINT  NOT NULL,
    child_bot_id INTEGER NOT NULL REFERENCES child_bots(id) ON DELETE CASCADE,
    selected_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (admin_id, child_bot_id)
);
-- Safe index: works whether column is still admin_id (pre-migration) or already owner_id (post-migration)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='ga_selected_bots' AND column_name='owner_id'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE tablename='ga_selected_bots' AND indexname='idx_ga_selected_bots_owner'
        ) THEN
            CREATE INDEX idx_ga_selected_bots_owner ON ga_selected_bots(owner_id);
        END IF;
    ELSE
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE tablename='ga_selected_bots' AND indexname='idx_ga_selected_bots_admin'
        ) THEN
            CREATE INDEX idx_ga_selected_bots_admin ON ga_selected_bots(admin_id);
        END IF;
    END IF;
END $$;

-- ════════════════════════════════════════════════════════════
-- Глобальные настройки (скидки и прочее)
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS global_settings (
    id SERIAL PRIMARY KEY,
    discount_percent INTEGER DEFAULT 0,
    discount_until TIMESTAMPTZ
);
INSERT INTO global_settings (id, discount_percent) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

-- Причина деактивации площадки: 'permissions' | 'kicked' | NULL
-- 'permissions' — бот в чате, но прав администратора не хватает → показываем инструкцию
-- 'kicked'      — бот удалён из чата
-- NULL          — площадка активна или причина не определена
ALTER TABLE bot_chats ADD COLUMN IF NOT EXISTS deactivation_reason VARCHAR(32);
