import json
import subprocess
import sys
from pathlib import Path


def get_apple_music_data():
    jxa_script = """
    var music = Application("Music");
    var tracks = music.libraryPlaylists[0].tracks.whose({playedCount: {_greaterThan: 0}});
    var count = tracks.length;
    var out = [];

    // Batch processing to avoid timeouts or memory issues if possible,
    // but typically getting properties as lists is faster in JXA.
    // However, mapping over 10k items can be slow.
    // Let's try getting parallel arrays which is often much faster in JXA than iterating objects.

    var names = tracks.name();
    var artists = tracks.artist();
    var albums = tracks.album();
    var playedDates = tracks.playedDate();
    var playedCounts = tracks.playedCount();

    for (var i = 0; i < names.length; i++) {
        // playedDates[i] is a Date object. Convert to ISO string if possible, or string.
        var d = playedDates[i];
        var dateStr = d ? d.toISOString() : null;

        out.push({
            name: names[i],
            artist: artists[i],
            album: albums[i],
            played_at: dateStr,
            play_count: playedCounts[i],
            source: "apple_music_library"
        });
    }

    JSON.stringify(out);
    """

    print("Fetching data from Apple Music (this may take a minute)...", file=sys.stderr)
    try:
        # Increase max buffer size just in case, though usually not needed for stdout capture this way
        result = subprocess.run(
            ["osascript", "-l", "JavaScript", "-e", jxa_script],
            capture_output=True,
            text=True,
            check=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error executing JXA: {e.stderr}", file=sys.stderr)
        return []

def main():
    data = get_apple_music_data()
    if not data:
        print("No data found or error occurred.")
        return

    output_path = Path("apple_music_history.jsonl")
    print(f"Writing {len(data)} records to {output_path}...")

    with output_path.open("w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")

    print("Done.")

if __name__ == "__main__":
    main()
