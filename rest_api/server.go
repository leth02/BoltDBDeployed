package main

import (
	"log"
	"fmt"
	"net/http"
	"github.com/boltdb/bolt"
	"encoding/json"
)

type CreateBucketPayload struct {
	Name string
}

type AddRequestPayload struct {
	Bucket string
	Key string
	Value string
}

type RetrieveRequestPayload struct {
	Bucket string
	Key string
}

type UpdateRequestPayload struct {
	Bucket string
	Key string
	Value string
}


type Message struct {
	Title string
	Content string
}


func main() {
	// Open the my.db data file in your current directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	// Handle 'create database' request (for now a database is automatically created above)
	// http.HandleFunc("/api/database/instance/create", createHandler)

	// Handle 'create bucket' request
	http.HandleFunc("/api/database/bucket/create", func(w http.ResponseWriter, r *http.Request) {
		db.Update(func(tx *bolt.Tx) error {

			var payload CreateBucketPayload

			decodeError := json.NewDecoder(r.Body).Decode(&payload)
			if decodeError != nil {
				http.Error(w, decodeError.Error(), http.StatusBadRequest)
				return fmt.Errorf("Failed to create a bucket: %s", decodeError)
			}

			_ , createBucketError := tx.CreateBucket([]byte(payload.Name))
			if createBucketError != nil {
				http.Error(w, fmt.Errorf("Failed to create a bucket: %s", createBucketError).Error(), http.StatusBadRequest)
				return fmt.Errorf("Failed to create a bucket: %s", createBucketError)
			}

			w.Header().Set("Content-Type", "application/json") 

			message := Message{"Success", "A new bucket is created"}
			json.NewEncoder(w).Encode(message)
			
			return nil

		})
	})

	// Handle 'add key-value to bucket' request
	http.HandleFunc("/api/database/data/add", func(w http.ResponseWriter, r *http.Request) {
		db.Update(func(tx *bolt.Tx) error {
			var payload AddRequestPayload

			decodeError := json.NewDecoder(r.Body).Decode(&payload)
			if decodeError != nil {
				http.Error(w, decodeError.Error(), http.StatusBadRequest)
				return fmt.Errorf("Failed to create a bucket: %s", decodeError)
			}

			// save the key-value pair to the bucket
			bucket := tx.Bucket([]byte(payload.Bucket))
			addKVError := bucket.Put([]byte(payload.Key), []byte(payload.Value))

			if addKVError == nil {
				w.Header().Set("Content-Type", "application/json") 

				message := Message{"Success", "A new key-value pair is added"}
				json.NewEncoder(w).Encode(message)
				return nil
			}

			http.Error(w, fmt.Errorf("Failed to create a bucket: %s", addKVError).Error(), http.StatusBadRequest)
			return fmt.Errorf("Failed to create a bucket: %s", addKVError)

		})
	})

	// // Handle 'remove key-value from bucket' request
	// http.HandleFunc("/api/database/data/remove", removeHandler)


	// Handle 'Retrieve a value with a key in a bucket' request
	http.HandleFunc("/api/database/data/read", func(w http.ResponseWriter, r *http.Request) {
		db.View(func(tx *bolt.Tx) error {
			var payload RetrieveRequestPayload

			decodeError := json.NewDecoder(r.Body).Decode(&payload)
			if decodeError != nil {
				http.Error(w, decodeError.Error(), http.StatusBadRequest)
				return fmt.Errorf("Failed to create a bucket: %s", decodeError)
			}

			bucket := tx.Bucket([]byte(payload.Bucket))
			value := bucket.Get([]byte(payload.Key))

			fmt.Printf("The value is: %s\n", value)

			w.Header().Set("Content-Type", "application/json") 

			message := Message{"Value", string(value)}
			json.NewEncoder(w).Encode(message)

			return nil
		})
	})

	// Turn on database server on port 8998
	http.ListenAndServe(":8998", nil)
	
}