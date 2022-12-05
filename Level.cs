using System.Collections.Generic;

namespace Managing.Database.Objects.In.SQL.With.SMO
{
    public class Level
    {
        public string Name { get; set; }
        public List<Level> Levels { get; set; }
        public Level Parent { get; set; }

        public Level(string Name)
        {
            this.Name = Name;
            Levels = new List<Level>();
        }

        public void Add(Level lvl)
        {
            lvl.Parent = this;
            Levels.Add(lvl);
        }
    }
}
