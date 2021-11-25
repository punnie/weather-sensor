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
	SeaLevel float32 `json:"sea_level"`
	GroundLevel float32 `json:"grnd_level"`
}

type WindSpec struct {
	Speed float32 `json:"speed"`
	Degree float32 `json:"deg"`
	Gust float32 `json:"gust"`
}

type CloudSpec struct {
	All int `json:"all"`
}

type RainSpec struct {
	LastHour float32 `json:"1h"`
	Last3Hours float32 `json:"3h"`
}

type SnowSpec struct {
	LastHour float32 `json:"1h"`
	Last3Hours float32 `json:"3h"`
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
	Rain RainSpec `json:"rain"`
	Snow SnowSpec `json:"snow"`
	Timestamp int `json:"dt"`
	Sys SysSpec `json:"sys"`
	Timezone int `json:"timezone"`
	Id int `json:"id"`
	Name string `json:"name"`
	Cod int `json:"cod"`
}

var k = koanf.New(".")

func fetchWeather(location string) (WeatherResponse, error) {
	var res WeatherResponse

	baseUrl, err := url.Parse("https://api.openweathermap.org/data/2.5/weather")

	if err != nil {
		return res, err
	}

	params := url.Values{}
	params.Add("q", location)
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

func writeWeather(weather WeatherResponse, location string) error {
	client := influxdb2.NewClientWithOptions(k.String("influxdb.hostname"), k.String("influxdb.token"), influxdb2.DefaultOptions().SetBatchSize(20))

	defer client.Close()

	var pressure float32

	// We're interested in knowing the atmospheric pressure in the location
	if weather.Main.GroundLevel == 0 {
		pressure = weather.Main.Pressure
	} else {
		pressure = weather.Main.GroundLevel
	}

	writer := client.WriteAPI(k.String("influxdb.org"), k.String("influxdb.bucket"))

	p := influxdb2.NewPointWithMeasurement(k.String("influxdb.measurement")).
		AddTag("location", location).
		AddTag("city", weather.Name).
		AddTag("country", weather.Sys.Country).
		AddField("visibility", weather.Visibility).
		AddField("clouds", weather.Clouds.All).
		AddField("wind_speed", weather.Wind.Speed).
		AddField("wind_bearing", weather.Wind.Degree).
		AddField("wind_gusts", weather.Wind.Gust).
		AddField("rain_1h", weather.Rain.LastHour).
		AddField("rain_3h", weather.Rain.Last3Hours).
		AddField("snow_1h", weather.Snow.LastHour).
		AddField("snow_3h", weather.Snow.Last3Hours).
		AddField("humidity", weather.Main.Humidity).
		AddField("temperature", weather.Main.Temp).
		AddField("temperature_max", weather.Main.TempMax).
		AddField("temperature_min", weather.Main.TempMin).
		AddField("pressure", pressure)

	writer.WritePoint(p)
	writer.Flush()

	return nil
}

func main() {
	if err := k.Load(file.Provider("config.toml"), toml.Parser()); err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	log.Printf("Starting weather virtual sensor reporting each %d seconds...", k.Int("sensor.interval"))

	locations := k.Strings("weather_api.locations")

	if len(locations) < 1 {
		log.Fatal("Weather locations are empty! Aborting...")
	}

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
		for _, location := range locations {
			weather, err := fetchWeather(location)

			if err != nil {
				log.Printf("Error fetching the weather: %v\n", err)
			} else {
				log.Printf("Weather fetched for location '%s'", location)
				writeWeather(weather, location)
			}
		}

		select {
		case sig := <-sigs:
			log.Printf("Signal %v captured, exiting...", sig)
			os.Exit(0)
		case <-ticks:
			continue
		}
	}
}
