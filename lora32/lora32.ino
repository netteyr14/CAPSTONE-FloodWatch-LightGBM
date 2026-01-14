#include <WiFi.h>
#include <HTTPClient.h>
#include <DHT.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>

// -------------------------------
// Sensor Configuration
// -------------------------------
#define DHTPIN 4
#define DHTTYPE DHT11
DHT dht(DHTPIN, DHTTYPE);

// HC-SR04 Pins
#define TRIG_PIN 5
#define ECHO_PIN 18

// -------------------------------
// LCD Configuration
// -------------------------------
LiquidCrystal_I2C lcd(0x27, 16, 2); 

// -------------------------------
// WiFi Configuration
// -------------------------------
String ssid;
String password;

const char* serverBaseUrl = "http://192.168.1.6:8700"; 

// -------------------------------
// Node Information
// -------------------------------
int node_num = 1;
int site_num = 1;

// -------------------------------
// Timers
// -------------------------------
unsigned long lastPostMs = 0;
const unsigned long postIntervalMs = 60000;

unsigned long lastDhtReadMs = 0;
const unsigned long dhtIntervalMs = 2000;

unsigned long lastUltraReadMs = 0;
const unsigned long ultraIntervalMs = 1000;

// -------------------------------
// Helper Functions
// -------------------------------

String getInput(const String& label) {
  Serial.print(label);
  while (Serial.available() == 0) {
    delay(10);
  }
  String input = Serial.readStringUntil('\n');
  input.trim();
  return input;
}

void connectToWiFi() {
  Serial.println("*Attempting to connect to WiFi*...");
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid.c_str(), password.c_str());

  int retryCount = 0;
  while (WiFi.status() != WL_CONNECTED && retryCount < 40) {
    retryCount++;
    Serial.printf("⏳ Connecting... attempt %d\n", retryCount);
    delay(250);
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("==== WiFi connected! ====");
    Serial.print("📡 IP Address: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("==== WiFi connection failed. ====");
    WiFi.disconnect(true);
  }
}

// -------------------------------
// HC-SR04 Distance Function
// -------------------------------
float readDistanceCM() {
  digitalWrite(TRIG_PIN, LOW);
  delayMicroseconds(2);

  digitalWrite(TRIG_PIN, HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);

  long duration = pulseIn(ECHO_PIN, HIGH, 30000); // 30ms timeout

  if (duration == 0) {
    return -1;  // no echo
  }

  float distance = duration * 0.0343 / 2;
  return distance;
}

// -------------------------------
// Setup
// -------------------------------
void setup() {
  Serial.begin(115200);
  while (!Serial) { delay(10); }

  // Initialize sensors
  dht.begin();

  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);

  // LCD Setup
  Wire.begin(21, 22);
  lcd.init();
  lcd.backlight();
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("ESP32 Flood IoT");

  // WiFi setup
  Serial.println("\n--- ESP32 WiFi Setup ---");
  ssid = getInput("Enter WiFi SSID: ");
  password = getInput("Enter WiFi Password: ");
  Serial.println("\nConnecting to: " + ssid);
  connectToWiFi();
}

// -------------------------------
// Loop
// -------------------------------
void loop() {
  unsigned long now = millis();

  if (WiFi.status() != WL_CONNECTED && (now % 5000 < 50)) {
    connectToWiFi();
  }

  static float humidity = NAN;
  static float temperature = NAN;
  static float distanceCM = -1;

  // --- DHT Read ---
  if (now - lastDhtReadMs >= dhtIntervalMs) {
    lastDhtReadMs = now;
    humidity = dht.readHumidity();
    temperature = dht.readTemperature();
  }

  // --- HC-SR04 Read ---
  if (now - lastUltraReadMs >= ultraIntervalMs) {
    lastUltraReadMs = now;
    distanceCM = readDistanceCM();
  }

  // --- LCD Display ---
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("T:");
  lcd.print(temperature, 1);
  lcd.print("C H:");
  lcd.print(humidity, 0);
  lcd.print("%");

  lcd.setCursor(0, 1);
  lcd.print("Dist:");
  if (distanceCM >= 0) {
    lcd.print(distanceCM, 1);
    lcd.print("cm ");
  } else {
    lcd.print("NoEcho ");
  }

  // --- Post to Flask ---
  if (now - lastPostMs >= postIntervalMs) {
    lastPostMs = now;

    String url = String(serverBaseUrl) + "/node/insert_queue";

    String payload = "{";
    payload += "\"temperature\":" + String(temperature, 1) + ",";
    payload += "\"humidity\":" + String(humidity, 1) + ",";
    payload += "\"ultrasonic\":" + String(distanceCM, 1) + ",";
    payload += "\"node_num\":" + String(node_num) + ",";
    payload += "\"site_num\":" + String(site_num);
    payload += "}";

    Serial.println("\n--- Sending POST ---");
    Serial.println(payload);

    if (WiFi.status() == WL_CONNECTED) {
      WiFiClient client;
      HTTPClient http;
      if (http.begin(client, url)) {
        http.addHeader("Content-Type", "application/json");
        int code = http.POST(payload);
        String resp = http.getString();

        Serial.println("Response: " + resp);
        Serial.println("HTTP Code: " + String(code));

        http.end();
      }
    }
  }

  delay(200);
}
