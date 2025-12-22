local mosq   = require("mosquitto")
local socket = require("socket")
local lfs    = require("lfs")
local LOCAL_BROKER_HOST = "127.0.0.1"
local LOCAL_BROKER_PORT = 1883
local LOCAL_TOPIC       = "backup/CpoFlow"
local CLIENT_ID         = "mqtt_local_backup_service"
local CLOUD_PING_HOST   = "103.150.226.102"
local BACKUP_DIR        = "/usr/local/home/root/test/backup"
local KEEP_DAYS         = 30
local QOS           = 0
local KEEPALIVE     = 60
local LOOP_TIMEOUT  = 1 
local CHECK_INTERVAL = 30
local loop_counter = 0
local is_cloud_reachable = false

local function ensure_dir(path)
  local attr = lfs.attributes(path)
  if not attr then
    local success = lfs.mkdir(path)
    if success then
      print(string.format("[FS] Directory created: %s", path))
    else
      print(string.format("[ERROR] Failed to create directory: %s", path))
    end
  end
end

local function check_network_reachability(host, timeout)
  local client_test = mosq.new("mqtt_check_" .. tostring(os.time()))
  local is_connected = false
  
  -- Callback check
  client_test.ON_CONNECT = function(success)
    if success then is_connected = true end
  end

  pcall(function()
      local res = client_test:connect(host, 1883, 10)
      if res == mosq.MOSQ_ERR_SUCCESS or res == true then
          client_test:loop_start()
          -- Wait up to 2 seconds for CONNACK
          local start = os.time()
          while os.difftime(os.time(), start) < 3 do
              if is_connected then break end
              socket.sleep(0.1)
          end
          client_test:disconnect()
          pcall(client_test.loop_stop, client_test, true)
      end
  end)
  
  return is_connected
end

local function save_to_daily_file(topic, payload)
  ensure_dir(BACKUP_DIR)
  local date_str = os.date("%Y-%m-%d")
  local file_path = string.format("%s/%s.json", BACKUP_DIR, date_str)
  local timestamp = os.date("%Y-%m-%d %H:%M:%S")

  -- Ensure payload doesn't contain newlines (flatten it to keep NDJSON valid)
  payload = payload:gsub("\n", ""):gsub("\r", "")
  
  local line = payload .. "\n"
  
  local file = io.open(file_path, "a")
  if not file then
    print(string.format("[ERROR] Failed to open file for writing: %s", file_path))
    return
  end
  file:write(line)
  file:close()
end

local function cleanup_old_files()
  ensure_dir(BACKUP_DIR)
  local now = os.time()
  local removed_count = 0
  for file in lfs.dir(BACKUP_DIR) do
    if file ~= "." and file ~= ".." and file:match("%.json$") then
      local y, m, d = file:match("(%d+)%-(%d+)%-(%d+)%.json")
      if y then
        local file_time = os.time({year=tonumber(y), month=tonumber(m), day=tonumber(d)})
        local age_days = os.difftime(now, file_time) / (24 * 3600)
        
        if age_days > KEEP_DAYS then
          os.remove(BACKUP_DIR .. "/" .. file)
          print(string.format("[CLEANUP] Deleted %s (%.1f days old)", file, age_days))
          removed_count = removed_count + 1
        end
      end
    end
  end
  if removed_count > 0 then
    print(string.format("[CLEANUP] Completed. Total files deleted: %d", removed_count))
  end
end

local client_local = mosq.new(CLIENT_ID)
function client_local.ON_CONNECT()
  print(string.format("[LOCAL] Connected to %s:%d. Subscribing to %s", LOCAL_BROKER_HOST, LOCAL_BROKER_PORT, LOCAL_TOPIC))
  client_local:subscribe(LOCAL_TOPIC, QOS)
end
function client_local.ON_MESSAGE(mid, topic, payload)
  if not is_cloud_reachable then
    save_to_daily_file(topic, payload)
  else
  end
end
function client_local.ON_DISCONNECT()
  print("[LOCAL] Disconnected from broker. Retrying connection...")
end

local function main_loop()
  print("[SYSTEM] MQTT Backup Service Active (Conditional Backup).")
  ensure_dir(BACKUP_DIR)
  
  -- Initial check
  is_cloud_reachable = check_network_reachability(CLOUD_PING_HOST, 2) -- Fast initial check
  print(string.format("[SYSTEM] Initial Network Status: %s", is_cloud_reachable and "ONLINE" or "OFFLINE"))
  
  -- Create global client instance
  client_local:connect(LOCAL_BROKER_HOST, LOCAL_BROKER_PORT, KEEPALIVE)
  
  -- Load Uploader Module
  package.loaded["mqtt_uploader_client"] = nil -- Force reload to avoid stale cache
  local uploader = require("mqtt_uploader_client")
  
  if type(uploader) ~= "table" then
      print("[ERROR] Failed to load uploader module. Got type: " .. type(uploader))
      -- Fallback to dofile if require failed to return a table (sometimes happens with empty returns)
      -- This assumes mqtt_uploader_client.lua is in the same dir
      print("[SYSTEM] Attempting fallback with dofile...")
      uploader = dofile("./mqtt_uploader_client.lua")
  end
  
  while true do
    -- Run local loop
    local ok, err = pcall(client_local.loop, client_local, LOOP_TIMEOUT)
    if not ok then
      print(string.format("[ERROR] MQTT Loop error: %s", err))
      socket.sleep(2)
      client_local:reconnect()
    else
    
      loop_counter = loop_counter + 1
      if loop_counter >= (CHECK_INTERVAL / LOOP_TIMEOUT) then
        local status_now = check_network_reachability(CLOUD_PING_HOST, 3)
        if status_now ~= is_cloud_reachable then
            if status_now then
                print("[NETWORK] Connection RESTORED. Enabling Cloud Uploads.")
            else
                print("[NETWORK] Connection LOST. Switching to Local Backup only.")
            end
        end
        is_cloud_reachable = status_now
        cleanup_old_files()
        
        -- Try to upload if online (every 30s)
        if is_cloud_reachable then
             uploader.scan_and_upload()
        end
        
        loop_counter = 0
      end
    end
  end
end
main_loop()