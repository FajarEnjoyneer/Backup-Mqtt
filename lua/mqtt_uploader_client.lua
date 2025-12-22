local M = {}

local mosq   = require("mosquitto")
local socket = require("socket")
local lfs    = require("lfs")

local MQTT_CLOUD_HOST  = "103.150.226.102"
local MQTT_CLOUD_PORT  = 1883
local MQTT_CLOUD_TOPIC = "upload/data/backup"

local BACKUP_DIR       = "/usr/local/home/root/test/backup"
local UPLOADED_DIR     = "/usr/local/home/root/test/uploaded"

local QOS            = 1
local KEEPALIVE      = 60

local function ensure_dir(path)
  if not lfs.attributes(path) then lfs.mkdir(path) end
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

function M.scan_and_upload()
  local client = mosq.new("mqtt_uploader_client_" .. tostring(os.time()))
  
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
        end
      end
    end
  end

  if #files_to_upload == 0 then 
    -- Silent return if nothing to do
    return 
  end
  
  print(string.format("[UPLOADER] Found %d files ready to upload.", #files_to_upload))

  local connect_ok, res, err = pcall(function()
    return client:connect(MQTT_CLOUD_HOST, MQTT_CLOUD_PORT, KEEPALIVE)
  end)

  if not connect_ok or not res then
    print(string.format("[UPLOADER] Failed to connect to cloud broker: %s", tostring(err or res or "Unknown error")))
    return
  end
  
  -- Use background loop handling like the simple working script
  client:loop_start()
  
  -- Give it a moment to stabilize the network connection and complete handshake
  socket.sleep(2)

  print("[UPLOADER] Connected to cloud broker.")

  for _, filename in ipairs(files_to_upload) do
    local filepath = BACKUP_DIR .. "/" .. filename
    local file_handle = io.open(filepath, "r")

    if file_handle then
      print(string.format("[UPLOADER] Processing file: %s", filename))
      local line_count = 0
      local error_occurred = false
      -- Use the base topic so it matches the simple test script and user's subscriber
      local file_topic = MQTT_CLOUD_TOPIC -- .. "/" .. filename:gsub("%.json$", "")

      for line in file_handle:lines() do
        if #line > 0 then
          client:publish(file_topic, line, QOS, false)
          
          -- Background loop handles sending, we just wait
          -- Delay 5 seconds as requested by user
          socket.sleep(2)

          line_count = line_count + 1
        end
      end

      file_handle:close()

      if not error_occurred then
        print(string.format("[UPLOADER] Finished sending %d lines.", line_count))
        move_file(filepath, UPLOADED_DIR)
      else
        print("[UPLOADER] Upload failed mid-way. File not moved.")
        break
      end
    else
      print(string.format("[UPLOADER] Failed to open file: %s", filepath))
    end
  end

  client:disconnect()
  pcall(client.loop_stop, client, true)
  print("[UPLOADER] Upload session finished.")
end

return M
