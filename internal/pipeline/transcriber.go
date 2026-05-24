package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type Transcriber struct {
	whisperURL    string
	initialPrompt string
}

func NewTranscriber(whisperURL, initialPrompt string) *Transcriber {
	return &Transcriber{whisperURL: whisperURL, initialPrompt: initialPrompt}
}

type whisperResponse struct {
	Text string `json:"text"`
}

type whisperVerboseResponse struct {
	Text     string `json:"text"`
	Segments []struct {
		AvgLogprob   float64 `json:"avg_logprob"`
		NoSpeechProb float64 `json:"no_speech_prob"`
	} `json:"segments"`
}

// TranscribeBytes sends raw WAV bytes to whisper without touching the filesystem.
func (t *Transcriber) TranscribeBytes(wav []byte) (string, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "audio.wav")
	if err != nil {
		return "", err
	}
	if _, err = fw.Write(wav); err != nil {
		return "", err
	}
	if t.initialPrompt != "" {
		mw.WriteField("initial_prompt", t.initialPrompt)
	}
	mw.Close()

	resp, err := http.Post(t.whisperURL+"/inference", mw.FormDataContentType(), &buf)
	if err != nil {
		return "", fmt.Errorf("whisper request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("whisper status %d: %s", resp.StatusCode, body)
	}

	var result whisperResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode whisper response: %w", err)
	}
	return result.Text, nil
}

func (t *Transcriber) Transcribe(audioPath string) (string, error) {
	f, err := os.Open(audioPath)
	if err != nil {
		return "", fmt.Errorf("open audio: %w", err)
	}
	defer f.Close()

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filepath.Base(audioPath))
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err = io.Copy(fw, f); err != nil {
		return "", fmt.Errorf("copy audio: %w", err)
	}
	if t.initialPrompt != "" {
		mw.WriteField("initial_prompt", t.initialPrompt)
	}
	mw.Close()

	resp, err := http.Post(t.whisperURL+"/inference", mw.FormDataContentType(), &buf)
	if err != nil {
		return "", fmt.Errorf("whisper request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("whisper status %d: %s", resp.StatusCode, body)
	}

	var result whisperResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode whisper response: %w", err)
	}

	return result.Text, nil
}

// TranscribeFiltered transcribes audioPath and applies the same hallucination
// guards used by the live audio pipeline (ClassifyStage). Returns ("", nil) for
// audio that is likely silence, background noise, or a Whisper hallucination so
// the caller can silently skip it without recording an error.
func (t *Transcriber) TranscribeFiltered(audioPath string) (string, error) {
	f, err := os.Open(audioPath)
	if err != nil {
		return "", fmt.Errorf("open audio: %w", err)
	}
	defer f.Close()

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filepath.Base(audioPath))
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err = io.Copy(fw, f); err != nil {
		return "", fmt.Errorf("copy audio: %w", err)
	}
	mw.WriteField("temperature", "0")
	mw.WriteField("response_format", "verbose_json")
	if t.initialPrompt != "" {
		mw.WriteField("initial_prompt", t.initialPrompt)
	}
	mw.Close()

	resp, err := http.Post(t.whisperURL+"/inference", mw.FormDataContentType(), &buf)
	if err != nil {
		return "", fmt.Errorf("whisper request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("whisper status %d: %s", resp.StatusCode, body)
	}

	var result whisperVerboseResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode whisper response: %w", err)
	}

	text := strings.TrimSpace(result.Text)
	if text == "" {
		return "", nil
	}

	// Confidence gate — same thresholds as ClassifyStage in the live pipeline.
	if len(result.Segments) > 0 {
		var sumLP float64
		var maxNSP float64
		for _, seg := range result.Segments {
			sumLP += seg.AvgLogprob
			if seg.NoSpeechProb > maxNSP {
				maxNSP = seg.NoSpeechProb
			}
		}
		avgLP := sumLP / float64(len(result.Segments))
		if avgLP < clarityAvgLogprobThresh || maxNSP > clarityNoSpeechProbThresh {
			return "", nil
		}
	}

	// Text-pattern filter — same patterns as ClassifyStage.
	for _, re := range hallucinationPatterns {
		if re.MatchString(text) {
			return "", nil
		}
	}

	return text, nil
}
