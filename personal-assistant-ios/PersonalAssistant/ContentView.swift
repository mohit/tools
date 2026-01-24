import SwiftUI

struct ContentView: View {
    @EnvironmentObject var healthViewModel: HealthViewModel
    @EnvironmentObject var devicesViewModel: DevicesViewModel
    @EnvironmentObject var googleServicesViewModel: GoogleServicesViewModel

    var body: some View {
        TabView {
            HealthView()
                .tabItem {
                    Label("Health", systemImage: "heart.fill")
                }

            DevicesView()
                .tabItem {
                    Label("Devices", systemImage: "iphone.and.laptop")
                }

            GoogleServicesView()
                .tabItem {
                    Label("Google", systemImage: "g.circle.fill")
                }
        }
    }
}

#Preview {
    ContentView()
        .environmentObject(HealthViewModel())
        .environmentObject(DevicesViewModel())
        .environmentObject(GoogleServicesViewModel())
}
