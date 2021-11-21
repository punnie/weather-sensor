package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/file"
)

type PointSpec struct {
	Longitude float32 `json:"lon"`
	Latitude float32 `json:"lat"`
}

type WeatherSpec struct {
	Id int `json:"id"`
	Main string `json:"main"`
	Description string `json:"description"`
	Icon string `json:"icon"`
}

type MainSpec struct {
	Temp float32 `json:"temp"`
	FeelsLike float32 `json:"feels_like"`
	TempMin float32 `json:"temp_min"`
	TempMax float32 `json:"temp_max"`
	Pressure float32 `json:"pressure"`
	Humidity float32 `json:"humidity"`
}

type WindSpec struct {
	Speed float32 `json:"speed"`
	Degree float32 `json:"deg"`
	Gust float32 `json:"gust"`
}

type CloudSpec struct {
	All int `json:"all"`
}

type SysSpec struct {
	Type int `json:"type"`
	Id int `json:"id"`
	Country string `json:"country"`
	Sunrise int `json:"sunrise"`
	Sunset int `json:"sunset"`
}

type WeatherResponse struct {
	Coordinates PointSpec `json:"coord"`
	Weather []WeatherSpec `json:"weather"`
	Base string `json:"base"`
	Main MainSpec `json:"main"`
	Visibility int `json:"visibility"`
	Wind WindSpec `json:"wind"`
	Clouds CloudSpec `json:"clouds"`
	Timestamp int `json:"dt"`
	Sys SysSpec `json:"sys"`
	Timezone int `json:"timezone"`
	Id int `json:"id"`
	Name string `json:"name"`
	Cod int `json:"cod"`
}

var k = koanf.New(".")

func fetchWeather() (WeatherResponse, error) {
	var res WeatherResponse

	baseUrl, err := url.Parse("https://api.openweathermap.org/data/2.5/weather")

	if err != nil {
		return res, err
	}

	params := url.Values{}
	params.Add("q", k.String("weather_api.api_location"))
	params.Add("appid", k.String("weather_api.appid"))
	params.Add("units", k.String("weather_api.units"))

	baseUrl.RawQuery = params.Encode()

	resp, err := http.Get(baseUrl.String())

	if err != nil {
		return res, err
	}

	defer resp.Body.Close()

	if resp.StatusCode / 100 == 2 {

		err := json.NewDecoder(resp.Body).Decode(&res)

		if err != nil {
			return res, err
		}

		return res, nil
	}

	return res, errors.New(fmt.Sprintf("Request failed with status: %d", resp.StatusCode))
}

func writeWeather(res WeatherResponse) error {
	client := influxdb2.NewClientWithOptions(k.String("influxdb.hostname"), k.String("influxdb.token"), influxdb2.DefaultOptions().SetBatchSize(20))

	defer client.Close()

	writer := client.WriteAPI(k.String("influxdb.org"), k.String("influxdb.bucket"))

	p := influxdb2.NewPointWithMeasurement(k.String("influxdb.measurement")).
		AddTag("location", k.String("weather_api.api_location")).
		AddField("temperature", res.Main.Temp).
		AddField("humidity", res.Main.Humidity).
		AddField("pressure", res.Main.Pressure)

	writer.WritePoint(p)
	writer.Flush()

	return nil
}

func main() {
	if err := k.Load(file.Provider("config.toml"), toml.Parser()); err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	log.Printf("Starting weather virtual sensor reporting each %d seconds...", k.Int("sensor.interval"))

	sigs := make(chan os.Signal)
	ticks := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			time.Sleep(time.Duration(k.Int("sensor.interval")) * time.Second)
			ticks <- true
		}
	}()

	for {
		weather, err := fetchWeather()

		if err != nil {
			log.Printf("Error fetching the weather: %v\n", err)
		}

		log.Printf("Weather fetched for location '%s'\n", k.String("weather_api.api_location"))

		writeWeather(weather)

		select {
		case sig := <-sigs:
			log.Printf("Signal %v captured, exiting...\n", sig)
			os.Exit(0)
			break
		case <-ticks:
			continue
		}
	}
}
