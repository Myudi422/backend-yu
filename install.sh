#!/bin/bash

# === DEFINISI DIREKTORI INSTALLASI ===
# Seluruh instalasi akan ditempatkan di /opt/.streaming
INSTALL_DIR="/opt/.streaming"

# === FUNGSIONALITAS CLEANUP (DIJALANKAN JIKA USER MENUTUP SCRIPT) ===
cleanup() {
    echo "🔹 Membersihkan instalasi..."
    # Hentikan proses backend dan frontend jika masih berjalan
    pkill -f "uvicorn app:app" 2>/dev/null
    pkill -f "bun run dev" 2>/dev/null
    # Hapus direktori instalasi jika ada
    if [ -d "$INSTALL_DIR" ]; then
        echo "Menghapus direktori instalasi: $INSTALL_DIR"
        sudo rm -rf "$INSTALL_DIR"
    fi
    echo "✅ Instalasi dihapus."
    exit 0
}

# Pasang trap: jika skrip menerima SIGINT, SIGTERM, atau EXIT, jalankan cleanup.
trap cleanup SIGINT SIGTERM EXIT

# === INSTAL CURL JIKA BELUM TERSEDIA ===
if ! command -v curl &> /dev/null; then
    echo "🔹 Curl tidak ditemukan. Menginstal curl..."
    sudo apt update -y && sudo apt install -y curl
fi

# === VALIDASI LICENSE BERDASARKAN EMAIL & TANGGAL ===
LICENSE_URL="https://raw.githubusercontent.com/Myudi422/backend-yu/refs/heads/main/user.txt"
tmp_license_file=$(mktemp)
curl -s -o "$tmp_license_file" "$LICENSE_URL"
if [ ! -s "$tmp_license_file" ]; then
    echo "❌ Gagal mengambil file license dari GitHub."
    exit 1
fi

read -p "Masukkan email Anda: " input_email
license_found=false
current_date=$(date +%Y-%m-%d)

while IFS='|' read -r email expiration_date; do
    # Lewati baris komentar atau kosong
    if [[ "$email" =~ ^#.* || -z "$email" ]]; then
        continue
    fi
    if [ "$email" = "$input_email" ]; then
        license_found=true
        if [[ "$current_date" > "$expiration_date" ]]; then
            echo "❌ License untuk email '$email' telah kadaluarsa (expired pada $expiration_date)."
            rm "$tmp_license_file"
            exit 1
        else
            echo "✅ License valid untuk email '$email'. Berlaku sampai $expiration_date."
        fi
        break
    fi
done < "$tmp_license_file"

rm "$tmp_license_file"

if [ "$license_found" = false ]; then
    echo "❌ Email '$input_email' tidak ditemukan dalam file license."
    exit 1
fi

# === FUNGSI RUN_COMMAND (NON-VERBOSE) ===
# Perintah dijalankan secara diam-diam, outputnya dialihkan ke /dev/null.
run_command() {
    # Jika diperlukan untuk debugging, hapus ">/dev/null 2>&1" pada baris di bawah.
    eval "$1" >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "❌ Gagal menjalankan: $1"
        exit 1
    fi
}

# === INSTALASI DEPENDENSI DAN SETUP SISTEM ===

run_command "sudo apt update -y && sudo apt upgrade -y"

if ! command -v python3.12 &> /dev/null; then
    run_command "sudo add-apt-repository ppa:deadsnakes/ppa -y"
    run_command "sudo apt update"
    run_command "sudo apt install -y python3.12 python3.12-venv python3.12-dev python3-pip"
fi

sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1
sudo update-alternatives --config python3 <<< "1"

if ! command -v node &> /dev/null || [[ $(node -v) != "v22."* ]]; then
    run_command "curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -"
    run_command "sudo apt install -y nodejs"
fi

if ! command -v bun &> /dev/null; then
    run_command "curl -fsSL https://bun.sh/install | bash"
    export BUN_INSTALL="$HOME/.bun"
    export PATH="$BUN_INSTALL/bin:$PATH"
fi

if ! command -v ffmpeg &> /dev/null; then
    run_command "sudo apt install -y ffmpeg"
fi

if ! command -v git &> /dev/null; then
    run_command "sudo apt install -y git"
fi

# === ATUR FIREWALL UNTUK PORT 3000 & 8000 ===
echo "🔹 Menambahkan aturan firewall untuk port 3000 dan 8000..."
sudo ufw allow 3000/tcp >/dev/null 2>&1
sudo ufw allow 8000/tcp >/dev/null 2>&1

# === HENTIKAN PROSES YANG SEDANG BERJALAN (JIKA ADA) ===
pkill -f "bun run dev" 2>/dev/null
pkill -f "uvicorn app:app" 2>/dev/null

# === SETUP DIREKTORI TERSEMBUNYI ===

if [ ! -d "$INSTALL_DIR" ]; then
    sudo mkdir -p "$INSTALL_DIR"
    sudo chown "$USER":"$USER" "$INSTALL_DIR"
fi

cd "$INSTALL_DIR" || exit

# Buat direktori log tersembunyi dan sembunyikan isinya
LOGS_DIR="$INSTALL_DIR/.logs"
if [ ! -d "$LOGS_DIR" ]; then
    mkdir "$LOGS_DIR"
    sudo chown root:root "$LOGS_DIR"
    sudo chmod 700 "$LOGS_DIR"
fi

# === INSTALASI BACKEND (TERSEMBUNYI) ===
BACKEND_DIR="$INSTALL_DIR/.backend-yu"
if [ ! -d "$BACKEND_DIR" ]; then
    run_command "git clone https://github.com/Myudi422/backend-yu.git \"$BACKEND_DIR\""
else
    cd "$BACKEND_DIR" || exit
    run_command "git pull origin main"
    cd "$INSTALL_DIR" || exit
fi

cd "$BACKEND_DIR" || exit
run_command "python3 -m venv venv"
source venv/bin/activate
run_command "pip3 install --upgrade pip"
run_command "pip3 install -r requirements.txt"
# Jalankan backend di background, output dialihkan ke direktori log tersembunyi
run_command "uvicorn app:app --host=0.0.0.0 --port=8000 --log-level=debug > \"$LOGS_DIR/backend.log\" 2>&1 &"

cd "$INSTALL_DIR" || exit

# === INSTALASI FRONTEND (TERSEMBUNYI) ===
FRONTEND_DIR="$INSTALL_DIR/.fronted-yu"
if [ ! -d "$FRONTEND_DIR" ]; then
    run_command "git clone https://github.com/Myudi422/fronted-yu.git \"$FRONTEND_DIR\""
else
    cd "$FRONTEND_DIR" || exit
    run_command "git pull origin main"
    cd "$INSTALL_DIR" || exit
fi

cd "$FRONTEND_DIR" || exit
if command -v bun &> /dev/null; then
    run_command "bun install"
else
    run_command "npm install"
fi
# Jalankan frontend di background, output dialihkan ke direktori log tersembunyi
run_command "bun run dev > \"$LOGS_DIR/frontend.log\" 2>&1 &"

cd "$INSTALL_DIR" || exit

# Pesan akhir yang tetap terlihat user
echo "🎉 Instalasi selesai. Backend & Frontend sudah berjalan!"
echo "🔄 Skrip akan tetap berjalan. Tekan Ctrl+C untuk menghentikan."

while true; do
    sleep 60
done
