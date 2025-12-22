local mosq   = require("mosquitto")
local socket = require("socket")
local lfs    = require("lfs")

local MQTT_CLOUD_HOST  = "test.mosquitto.org"
local MQTT_CLOUD_PORT  = 1883
local MQTT_CLOUD_TOPIC = "upload/data/backup"

local BACKUP_DIR       = "/usr/local/home/root/test/backup"
local UPLOADED_DIR     = "/usr/local/home/root/test/uploaded"

local QOS            = 1
local KEEPALIVE      = 60
local CHECK_INTERVAL = 10

local client = mosq.new("mqtt_uploader_client")

local function ensure_dir(path)
  if not lfs.attributes(path) then lfs.mkdir(path) end
end

local function check_network_reachability(host)
  local cmd = string.format("ping -c 1 -W 2 %s > /dev/null 2>&1", host)
  return (os.execute(cmd) == 0)
end

local function move_file(src_path, dest_dir)
  ensure_dir(dest_dir)
  local filename = src_path:match("([^/]+)$")
  local dest_path = dest_dir .. "/" .. filename
  local success, err = os.rename(src_path, dest_path)
  if success then
    print(string.format("[MOVE] File moved to: %s", dest_path))
    return true
  else
    print(string.format("[ERROR] Failed to move file %s: %s", filename, tostring(err)))
    return false
  end
end

local function process_upload_queue()
  ensure_dir(BACKUP_DIR)
  ensure_dir(UPLOADED_DIR)

  local files_to_upload = {}
  local current_time = os.time()

  for file in lfs.dir(BACKUP_DIR) do
    if file:match("%.json$") then
      local filepath = BACKUP_DIR .. "/" .. file
      local attr = lfs.attributes(filepath)
      -- Check if it's a file and old enough (older than 60s)
      if attr and attr.mode == "file" then
        if (current_time - attr.modification) > 60 then
          table.insert(files_to_upload, file)
        else
          -- Optional: print debug if needed, checking silenced to avoid spam
        end
      end
    end
  end

  if #files_to_upload == 0 then return end
  print(string.format("[SYSTEM] Found %d files ready to upload. Starting upload...", #files_to_upload))

  local connect_ok, res, err = pcall(function()
    return client:connect(MQTT_CLOUD_HOST, MQTT_CLOUD_PORT, KEEPALIVE)
  end)

  if not connect_ok or not res then
    print(string.format("[ERROR] Failed to connect to broker: %s", tostring(err or res or "Unknown error")))
    return
  end
  
  -- Give it a moment to stabilize the network connection
  socket.sleep(1)

  local loop_ok = false
  for _=1,5 do
    if client:loop(0) == mosq.MOSQ_ERR_SUCCESS then loop_ok = true; break end
    socket.sleep(0.5)
  end
  if not loop_ok then
    print("[ERROR] MQTT connection unstable.")
    return
  end

  print("[MQTT] Connected to cloud broker.")

  for _, filename in ipairs(files_to_upload) do
    local filepath = BACKUP_DIR .. "/" .. filename
    local file_handle = io.open(filepath, "r")

    if file_handle then
      print(string.format("[UPLOAD] Processing file: %s", filename))
      local line_count = 0
      local error_occurred = false
      local file_topic = MQTT_CLOUD_TOPIC .. "/" .. filename:gsub("%.json$", "")

      for line in file_handle:lines() do
        if #line > 0 then
          client:publish(file_topic, line, QOS, false)
          
          -- Wait a small amount to allow socket flush/processing (15ms)
          local loop_res = client:loop(15)
          
          if loop_res ~= mosq.MOSQ_ERR_SUCCESS then
            print("[ERROR] Connection lost during upload.")
            error_occurred = true
            break
          end
          
          -- Additional small sleep to limit rate if needed, keeping it stable
          socket.sleep(0.01)

          line_count = line_count + 1
        end
      end

      file_handle:close()

      if not error_occurred then
        -- Final flush
        client:loop(100)
        print(string.format("[UPLOAD] Finished sending %d lines.", line_count))
        move_file(filepath, UPLOADED_DIR)
      else
        print("[UPLOAD] Upload failed mid-way. File not moved.")
        -- Stop processing further files in this batch if one failed,
        -- to prevent long timeouts on every file if network is down.
        break
      end
    else
      print(string.format("[ERROR] Failed to open file: %s", filepath))
    end
  end

  client:disconnect()
  print("[MQTT] Upload session finished. Disconnected.")
end

local function main()
  print("[SYSTEM] MQTT Uploader started.")
  print("[SYSTEM] Source Dir: " .. BACKUP_DIR)

  while true do
    if check_network_reachability(MQTT_CLOUD_HOST) then
      process_upload_queue()
    else
      print(string.format("[PING] Host %s OFFLINE. Waiting %d seconds...", MQTT_CLOUD_HOST, CHECK_INTERVAL))
    end
    socket.sleep(CHECK_INTERVAL)
  end
end

main()
